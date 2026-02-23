/*
 * Copyright 2025 Snowflake Inc.
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

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <unistd.h>

#include "postgres.h"

#include "utils/pidfile.h"
#define MAXPGPATH 1024

static int	my_pidfile;
static char	pidfile_path[MAXPGPATH];

/**
 * @brief Creates and locks a PID file.
 */
static int
create_pid_file(const char *path)
{
	/* Open the file with flags that ensure atomic creation. */
	/* O_WRONLY: Open for writing only. */
	/* O_CREAT: Create the file if it does not exist. */
	/* O_EXCL: When used with O_CREAT, fail if the file already exists. */
	/* This combination prevents race conditions where two processes check for */
	/* the file's existence and then try to create it simultaneously. */
	/* The permissions 0644 mean read/write for the owner and read-only for */
	/* group and others. */
	int			fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0644);

	if (fd == -1)
	{
		perror("Failed to create PID file");

		/*
		 * EEXIST is a common error here, indicating another instance is
		 * running.
		 */
		return -1;
	}

	/* Try to acquire an exclusive, non-blocking lock on the file. */
	/* LOCK_EX: Acquire an exclusive lock. */
	/* LOCK_NB: Non-blocking; if the lock can't be acquired, it returns -1 */
	/* immediately instead of waiting. */
	if (flock(fd, LOCK_EX | LOCK_NB) == -1)
	{
		perror("Failed to lock PID file (another instance may be running)");
		close(fd);
		/* Clean up the created file descriptor */
		unlink(path);
		/* Delete the file we just created */
		/* EWOULDBLOCK indicates that the lock is held by another process. */
		return -1;
	}

	/* Get the current process ID. */
	pid_t		pid = getpid();
	char		pid_str[16];

	/* Format the PID into a string. */
	snprintf(pid_str, sizeof(pid_str), "%d\n", pid);

	/* Write the PID string to the file. */
	if (write(fd, pid_str, strlen(pid_str)) == -1)
	{
		perror("Failed to write to PID file");
		/* If we can't write, perform cleanup. */
		flock(fd, LOCK_UN);
		/* Release the lock */
		close(fd);
		unlink(path);
		return -1;
	}

	/* Return the file descriptor. The lock will be held as long as this */
	/* descriptor is open. */
	return fd;
}

/**
 * @brief Removes the PID file and releases the lock.
 */
static void
remove_pid_file(int fd, const char *path)
{
	if (fd < 0)
	{
		return;
	}

	/* Release the lock. */
	if (flock(fd, LOCK_UN) == -1)
	{
		perror("Failed to unlock PID file");
	}

	/* Close the file descriptor. */
	if (close(fd) == -1)
	{
		perror("Failed to close PID file");
	}

	/* Delete the file from the filesystem. */
	if (unlink(path) == -1)
	{
		perror("Failed to remove PID file");
	}
}


/* register this pidfile */
void
create_my_pidfile(const char *path)
{
	my_pidfile = create_pid_file(path);
	strlcpy(pidfile_path, path, MAXPGPATH);
}

/* atexit handler */
void
cleanup_my_pidfile()
{
	remove_pid_file(my_pidfile, pidfile_path);
}
