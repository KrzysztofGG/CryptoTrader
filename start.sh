#!/bin/bash

docker compose down -v
mvn clean package
docker compose up -d
docker exec -it $(docker ps | grep jobmanager | awk '{ print $1 }') flink run /opt/flink/job.jar