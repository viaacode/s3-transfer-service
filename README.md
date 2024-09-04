# S3 Transfer Service

## Synopsis

Lightweight service that will transfer files from the object store to a remote server.

We don't want any traffic in the service itself, so the file is transferred from the source URL to the server directly and not via the service in between. Connection to the remote server is via SSH and uses the higher-level SFTP protocol where possible. In the other cases, shell commands are executed.

A transfer will be split up in multiple parts of the same size. Each part will be transferred simultaneously, running in a separate thread. Afterwards
the file will be assembled into the destination file.

There is an optional free space check in which the remote server needs to have a certain amount of free space (in percentage) before the file is transferred. If that free space is not met, the service will sleep indefinitely until the space is freed. The information needed for the check is defined by the ENV vars: `SSH_FREE_SPACE_PERCENTAGE`, `SSH_FILE_SYSTEM`. The latter is the mountpoint of the filesystem to check the free space of. The vars are mandatory but the values may be empty. In fact, if at least one of the values are empty than the check will not be executed, and the file will be transferred regardless of the free space on the target server.

## Prerequisites

- Git
- Docker (optional)
- Python 3.10+
- Access to the [meemoo PyPi](http://do-prd-mvn-01.do.viaa.be:8081)
- Poetry

## Diagrams

<details>
  <summary>Sequence diagram (click to expand)</summary>

  ![S3 Transfer Service](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/viaacode/s3-transfer-service/main/docs/s3-t-s_sequence-diagram.plantuml&fmt=svg)

</details>

## Usage

1. Clone this repository with:

   `$ git clone https://github.com/viaacode/s3-transfer-service.git`

2. Change into the new directory.

3. Set the needed config:

    Included in this repository is a `config.yml` file detailing the required configuration.
    There is also an `.env.example` file containing all the needed env variables used in the `config.yml` file.
    All values in the config have to be set in order for the application to function correctly.
    You can use `!ENV ${EXAMPLE}` as a config value to make the application get the `EXAMPLE` environment variable.

### Running locally
1. Install the external modules:

    `$ poetry install`

2. Run the tests (and setting the values in `.env.example` first):

    `$ export $(grep -v '^#' .env.example | xargs -0); poetry run pytest -v --cov=./app`

3. Run the application:

    `$ poetry run python main.py`

### Running using Docker

1. Build the container:

   `$ docker build -t s3-transfer-service:latest .`

2. Run the test container:

   `$ docker run --env-file .env.example --rm --entrypoint python s3-transfer-service:latest -m pytest -v --cov=./app`

2. Run the container (with specified `.env` file):

   `$ docker run --env-file .env --rm s3-transfer-service:latest`