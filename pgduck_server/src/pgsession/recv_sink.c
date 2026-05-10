/*
 * Copyright 2026 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Receive sink implementation. See pgsession/recv_sink.h for the
 * conceptual overview.
 *
 * The sink is a regular file under a server-private base directory. Each
 * client connection gets its own sink at most one at a time; the path is
 * unique per (pid, monotonic seq, time) tuple and the directory is mode
 * 0700 to keep other local users out. Stale files from a crashed prior
 * run are swept on startup.
 */
#include "pgsession/recv_sink.h"

#include "c.h"
#include "common/fe_memutils.h"
#include "utils/pgduck_log_utils.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

struct ReceiveSink
{
	char	   *path;
	int			fd;				/* -1 once finalized or destroyed */
};

static char *globalBaseDir = NULL;
static pthread_mutex_t sinkSeqLock = PTHREAD_MUTEX_INITIALIZER;
static unsigned long sinkSeq = 0;

static int	sweep_directory(const char *dir);
static int	mkdir_p(const char *path);

int
recv_sink_global_init(const char *base_dir)
{
	struct stat st;

	if (base_dir == NULL || base_dir[0] == '\0')
	{
		errno = EINVAL;
		PGDUCK_SERVER_ERROR("recv_sink_global_init: empty base_dir");
		return -1;
	}

	/*
	 * mkdir -p semantics: the derived default <cache_dir>/recv often runs
	 * before <cache_dir> exists (DuckDB creates it lazily on first cache
	 * write), so a plain mkdir would fail with ENOENT.
	 */
	if (mkdir_p(base_dir) != 0)
	{
		PGDUCK_SERVER_ERROR("could not create recv dir %s: %s",
							base_dir, strerror(errno));
		return -1;
	}

	if (stat(base_dir, &st) != 0)
	{
		PGDUCK_SERVER_ERROR("could not stat recv dir %s: %s",
							base_dir, strerror(errno));
		return -1;
	}
	if (!S_ISDIR(st.st_mode))
	{
		PGDUCK_SERVER_ERROR("recv dir %s is not a directory", base_dir);
		errno = ENOTDIR;
		return -1;
	}

	/*
	 * Tighten permissions even on a pre-existing directory; we are the only
	 * intended writer/reader.
	 */
	if ((st.st_mode & 0777) != 0700)
	{
		if (chmod(base_dir, 0700) != 0)
		{
			PGDUCK_SERVER_ERROR("could not chmod recv dir %s to 0700: %s",
								base_dir, strerror(errno));
			return -1;
		}
	}

	(void) sweep_directory(base_dir);

	if (globalBaseDir != NULL)
		free(globalBaseDir);
	globalBaseDir = strdup(base_dir);
	if (globalBaseDir == NULL)
	{
		PGDUCK_SERVER_ERROR("out of memory recording recv dir");
		return -1;
	}

	return 0;
}

const char *
recv_sink_global_dir(void)
{
	return globalBaseDir;
}

/*
 * Recursive mkdir, like `mkdir -p`. Tolerates pre-existing leaf and
 * intermediate directories. Returns 0 on success; -1 with errno set on
 * any other failure.
 */
static int
mkdir_p(const char *path)
{
	char	   *tmp;
	size_t		len;
	char	   *p;

	if (path == NULL || path[0] == '\0')
	{
		errno = EINVAL;
		return -1;
	}

	len = strlen(path);
	tmp = strdup(path);
	if (tmp == NULL)
		return -1;

	/* Strip trailing slash so the final-component mkdir below isn't a no-op. */
	if (tmp[len - 1] == '/')
		tmp[len - 1] = '\0';

	for (p = tmp + 1; *p != '\0'; p++)
	{
		if (*p == '/')
		{
			*p = '\0';
			if (mkdir(tmp, 0700) != 0 && errno != EEXIST)
			{
				int			saved_errno = errno;

				free(tmp);
				errno = saved_errno;
				return -1;
			}
			*p = '/';
		}
	}
	if (mkdir(tmp, 0700) != 0 && errno != EEXIST)
	{
		int			saved_errno = errno;

		free(tmp);
		errno = saved_errno;
		return -1;
	}
	free(tmp);
	return 0;
}

/*
 * Best-effort sweep: unlink every regular file in dir. Subdirectories are
 * left alone (we never create any). Failures are logged but not fatal —
 * worst case is we leak a few KB of stale files until the next sweep.
 */
static int
sweep_directory(const char *dir)
{
	DIR		   *d;
	struct dirent *de;
	int			removed = 0;
	int			failed = 0;

	d = opendir(dir);
	if (d == NULL)
	{
		PGDUCK_SERVER_WARN("could not open recv dir %s for sweep: %s",
						   dir, strerror(errno));
		return -1;
	}

	while ((de = readdir(d)) != NULL)
	{
		char		path[PATH_MAX];

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
		if (unlink(path) == 0)
			removed++;
		else
			failed++;
	}
	closedir(d);

	if (removed > 0 || failed > 0)
		PGDUCK_SERVER_LOG("recv dir sweep: removed %d, failed %d",
						  removed, failed);

	return 0;
}

ReceiveSink *
recv_sink_create(void)
{
	ReceiveSink *sink;
	char	   *path = NULL;
	int			fd;
	unsigned long seq;
	pid_t		pid = getpid();

	if (globalBaseDir == NULL)
	{
		PGDUCK_SERVER_ERROR("recv_sink not initialized");
		errno = EINVAL;
		return NULL;
	}

	pthread_mutex_lock(&sinkSeqLock);
	seq = ++sinkSeq;
	pthread_mutex_unlock(&sinkSeqLock);

	if (asprintf(&path, "%s/recv_%d_%lu_%lld",
				 globalBaseDir, (int) pid, seq,
				 (long long) time(NULL)) < 0)
	{
		PGDUCK_SERVER_ERROR("could not format recv path: %s", strerror(errno));
		return NULL;
	}

	/*
	 * O_EXCL guards against any pre-existing file collision (the seq is
	 * monotonic per process so a collision implies an attacker placed a file
	 * there; refuse to use it).
	 */
	fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
	{
		PGDUCK_SERVER_ERROR("could not create recv file %s: %s",
							path, strerror(errno));
		free(path);
		return NULL;
	}

	sink = pg_malloc(sizeof(*sink));
	sink->path = path;
	sink->fd = fd;
	return sink;
}

const char *
recv_sink_path(const ReceiveSink * sink)
{
	return sink ? sink->path : NULL;
}

int
recv_sink_write(ReceiveSink * sink, const char *data, size_t len)
{
	if (sink == NULL || sink->fd < 0)
	{
		errno = EBADF;
		return -1;
	}

	while (len > 0)
	{
		ssize_t		n = write(sink->fd, data, len);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			PGDUCK_SERVER_ERROR("recv sink write failed on %s: %s",
								sink->path, strerror(errno));
			return -1;
		}
		data += n;
		len -= (size_t) n;
	}
	return 0;
}

int
recv_sink_finalize(ReceiveSink * sink)
{
	if (sink == NULL || sink->fd < 0)
		return 0;

	if (close(sink->fd) != 0)
	{
		PGDUCK_SERVER_ERROR("could not close recv sink %s: %s",
							sink->path, strerror(errno));
		sink->fd = -1;
		return -1;
	}
	sink->fd = -1;
	return 0;
}

void
recv_sink_destroy(ReceiveSink * sink)
{
	if (sink == NULL)
		return;

	if (sink->fd >= 0)
		(void) close(sink->fd);
	if (sink->path != NULL)
	{
		(void) unlink(sink->path);
		free(sink->path);
	}
	pg_free(sink);
}
