#!/usr/bin/env bash

## initialising environment
## https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#initializing-environment
# remove docker-compose services
docker-compose down --volumes
# create folders to mount
rm -rf ./dags ./logs ./plugins .env
mkdir -p ./dags ./logs ./plugins
# setting the right airflow user
echo -e "AIRFLOW_UID=$(id -u)" > .env
# initialise database
docker-compose up airflow-init
