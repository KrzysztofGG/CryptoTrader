package org.example.sink;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.model.TradeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxBalanceSink extends RichSinkFunction<TradeEvent> {

    private static final Logger LOG =
            LoggerFactory.getLogger(InfluxBalanceSink.class);

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

        LOG.info(
                "Influx sink initialized: url={}, org={}, bucket={}",
                url, org, bucket
        );
    }

    @Override
    public void invoke(TradeEvent event, Context context) {
        try {
            Point point = Point.measurement("trade_balance")
                    .addField("balance", event.balance)
                    .addField("price", event.price)
                    .addField("pnl", event.pnl)
                    .addField("pnlPercent", event.pnlPercent)
                    .addTag("symbol", event.symbol)
                    .addTag("action", event.action);

            writeApi.writePoint(point);
        } catch (Exception e) {
            LOG.error("Error writing TradeEvent to InfluxDB", e);
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