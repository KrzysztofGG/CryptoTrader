# CryptoTrader

### Steps to run
- `docker compose up -d` (to initialize influxdb at localhost:8086)
- `mvn clean package` (build jar)
- `java -jar target/CryptoTrader-1.0-SNAPSHOT.jar` (run java program)

### Interactions
You can observe what trader is doing by:
- checking terminal window (very limited, mostly used to make sure program is healthy and running)
- using Influxdb UI at localhost:8086
