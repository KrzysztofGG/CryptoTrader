package org.example.sink;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.model.TradeEvent;

import java.time.Instant;

public class InfluxBalanceSink extends RichSinkFunction<TradeEvent> {

    private transient InfluxDBClient influxDBClient;
    private transient WriteApiBlocking writeApi;
    private final String url;
    private final String token;
    private final String org;
    private final String bucket;

    public InfluxBalanceSink(String url, String token, String org, String bucket) {
        this.url = url;
        this.token = token;
        this.org = org;
        this.bucket = bucket;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Create InfluxDB 2.x client
        influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
        writeApi = influxDBClient.getWriteApiBlocking();
    }

    @Override
    public void invoke(TradeEvent event, Context context) {
        try {
            Point point = Point.measurement("trade_balance")
                    .time(Instant.ofEpochMilli(event.timestamp), WritePrecision.MS)
                    .addField("balance", event.balance)
                    .addField("price", event.price)
                    .addField("pnl", event.pnl)
                    .addField("pnlPercent", event.pnlPercent)
                    .addTag("symbol", event.symbol)
                    .addTag("action", event.action);

            writeApi.writePoint(point);
        } catch (Exception e) {
            System.err.println("Error writing to InfluxDB: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (influxDBClient != null) {
            influxDBClient.close();
        }
    }
}