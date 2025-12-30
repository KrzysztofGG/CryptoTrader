# CryptoTrader

### Prerequisites
- java 17
- maven
- .env file (with missing values filled):
```bash
INFLUXDB_URL=http://influxdb:8086
OUTPUT_FILE_PATH=/data/output.csv
RUNNING_MODE=csv # or db

# InfluxDB 2.x bootstrap
DOCKER_INFLUXDB_INIT_MODE=setup
DOCKER_INFLUXDB_INIT_USERNAME=
DOCKER_INFLUXDB_INIT_PASSWORD=
DOCKER_INFLUXDB_INIT_ORG=trading 
DOCKER_INFLUXDB_INIT_BUCKET=trading_db
DOCKER_INFLUXDB_INIT_ADMIN_TOKEN= 
```

### How to run:
- `mvn clean package`
- `docker compose up -d`
- `docker exec -it $(docker ps | grep jobmanager | awk '{ print $1 }') flink run /opt/flink/job.jar`

Or run `./start.sh`

### How to observe app working
- Program logs available in container: `docker logs -f cryptotrader-taskmanager`
- If using CSV mode, values are sinked to location defined in OUTPUT_FILE_PATH environment variable
- If using DB mode, access InfluxDB UI at `localhost:8086`