## Introduction

This tool is designed to synchronize data between a Postgresql database and Elasticsearch. It uses a trigger system with a pub/sub mechanism to monitor real-time changes in the database and index them in Elasticsearch.



## Installation and Configuration

### Using Docker

- Pull the Docker image from the GitHub Container Registry:
    ```bash
    docker pull ghcr.io/quix-labs/pg-el-sync:latest
    ```

### Using Go

- Install the tool using Go:
    ```bash
    go install github.com/quix-labs/pg-el-sync@latest
    ```

### Using prebuilt assets

You can also install the tool using release assets such as `.deb`, `.apk`, or others.

Download the appropriate package from the [Releases page](https://github.com/quix-labs/pg-el-sync/releases), and then follow the instructions provided for your specific platform.


## Usage

The tool provides two main commands for usage:

- `pg-el-sync listen`: Start listening to the PostgreSQL database for real-time changes and sync them with Elasticsearch.
- `pg-el-sync index`: Index all tables from the PostgreSQL database into Elasticsearch.


### Using Docker

#### Listen Command
```bash
docker run -v /path/to/config.yaml:/app/config.yaml ghcr.io/quix-labs/pg-el-sync:latest pg-el-sync listen
```

#### Index Command
```bash
docker run --rm -v /path/to/config.yaml:/app/config.yaml ghcr.io/quix-labs/pg-el-sync:latest pg-el-sync index
```

### Configuration

#### Configuration file
By default `pg-el-sync` load configuration file from `/app/config.yaml`, you can override using environment variable:
  ```bash
    export CONFIG_FILE=/path/to/config.yaml
    # Or with docker: docker run -e CONFIG_FILE=/path/in/docker/config.yaml ...
  ``` 


## Supervisord Configuration

To manage the `pg-el-sync listen` command with Supervisord, you can use the following configuration:

```ini
[program:pg-el-sync]
command=/path/to/pg-el-sync listen 
autostart=true
autorestart=true
startsecs=10
startretries=3
stdout_logfile=/var/log/pg-el-sync-listen.log
stderr_logfile=/var/log/pg-el-sync-listen.err.log
```

Make sure to replace `/path/to/pg-el-sync` with the absolute path to your `pg-el-sync` installation.

You can also adjust other settings according to your needs, such as log file paths and startup parameters.


## Local Development

To set up the environment for local development, follow these steps:

1. Clone this repository:
    ```bash
    git clone https://github.com/quix-labs/pg-el-sync
    ```
2. Navigate to the `pg-el-sync` directory:
    ```bash
    cd pg-el-sync
    ```
3. Copy the `config.example.yaml` file to `config.yaml` and configure it according to your environment:
    ```bash
    cp config.example.yaml config.yaml
    ```

4. Run the tool in development mode:
    ```bash
    go run .
    ```


## Credits

- [COLANT Alan](https://github.com/alancolant)
- [All Contributors](../../contributors)

## License

This project is licensed under the [MIT License](LICENCE.md).