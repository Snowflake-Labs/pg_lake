## Prerequisites
1. Make sure to allocate >= ~20GB memory resource to docker.
2. Your `~/.aws` folder is mounted as a read only volume to pgduck-server container so that it can read/write buckets with your aws credentials. You are expected to set up your aws credentials before. (possible to read/write from/to **production** s3 buckets)

## How to run?
- Run `cd docker && docker compose up` to create all containers (minio, pgduck-server, and pg_lake-postgres) You can take a coffee break, this will take some time (~30 minutes on mac air m3) but a one time operation. If you want to build against different refs (default is main branch) or different postgres versions (default is 18), you can override them in [env file](.env).
- At the end, you can connect to postgres via `docker exec -it pg_lake psql`.

> [!WARNING]
> These containers are mainly used for test and development purposes. Be cautious when you run them in production environments.
