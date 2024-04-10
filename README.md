# apache-airflow-data-engineer

Apache Airflow For Data Engineers Tutorial

# Airflow Environment Setup

This project uses Apache Airflow to manage and schedule data pipelines. The project is containerized using Docker and
orchestrated using Docker Compose.

## Prerequisites

- Docker
- Docker Compose

## Setup

1. Clone the repository to your local machine.

2. Navigate to the project directory.

3. Build the Docker images:

```bash
docker-compose build
```

4. Start the Airflow services:

```bash
docker-compose up
```

## Configuration

The `docker-compose.yaml` file contains the configuration for the Airflow services. The following environment variables
are used:

- `AIRFLOW__CORE__EXECUTOR`: The executor to use for Airflow. In this project, we use the `CeleryExecutor`.
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: The connection string for the Airflow metadata database.
- `AIRFLOW__CELERY__RESULT_BACKEND`: The connection string for the backend that Celery uses for storing results.
- `AIRFLOW__CELERY__BROKER_URL`: The connection string for the message broker that Celery uses for sending tasks.
- `AIRFLOW__CORE__FERNET_KEY`: The Fernet key used for encrypting passwords in the connection configuration.
- `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`: Whether to pause DAGs when they are created.
- `AIRFLOW__CORE__LOAD_EXAMPLES`: Whether to load the example DAGs that come with Airflow.
- `AIRFLOW__API__AUTH_BACKENDS`: The authentication backends to use for the Airflow API.

## Running the Airflow Webserver

Once the services are up and running, you can access the Airflow webserver at `http://localhost:8080`.

## DAGs

The DAGs are defined in Python files in the `dags` directory.

## Data

The data for the DAGs is stored in CSV files in the `datasets` directory.

## Logs

The logs for the Airflow tasks are stored in the `logs` directory.

## Plugins

Any Airflow plugins can be added to the `plugins` directory.

## Stopping the Services

To stop the Airflow services, run:

```bash
docker-compose down
```

## Additional Information

For more information on Apache Airflow, see the [official documentation](https://airflow.apache.org/docs/). For more
information on Docker and Docker Compose, see the [Docker documentation](https://docs.docker.com/) and
the [Docker Compose documentation](https://docs.docker.com/compose/).