package org.example.main;


import org.apache.flink.api.common.functions.MapFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.config.FlinkEnvironmentConfig;
import org.example.model.Trade;
import org.example.model.TradeEvent;
import org.example.sink.CsvSink;
import org.example.sink.InfluxBalanceSink;
import org.example.source.BinanceWebSocketSource;
import org.example.strategy.CrossingMATrader;
import org.example.strategy.SimpleTrader;
import org.example.strategy.TradingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


public class Main {

    private static final Logger LOG =
            LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        Map<String, String> envVars = System.getenv();
        ParameterTool envParams = ParameterTool.fromMap(envVars);

        ParameterTool argsParams = ParameterTool.fromArgs(args);

        // CLI args override env vars
        ParameterTool params = envParams.mergeWith(argsParams);
        validateConfig(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        FlinkEnvironmentConfig.configure(env);

        String symbol = params.get("symbol", "btcusdt");
        String influxUrl = params.getRequired("INFLUXDB_URL");
        String influxToken = params.getRequired("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN");
        String runningMode = params.getRequired("RUNNING_MODE");
        String strategy = params.getRequired("STRATEGY");
        String influxOrg = params.get("DOCKER_INFLUXDB_INIT_ORG", "trading");
        String influxBucket = params.get("DOCKER_INFLUXDB_INIT_BUCKET", "trading_db");

        TradingStrategy trader = null;
        if ("simple".equals(strategy)) {
            trader = new SimpleTrader();
        } else if ("crossing_ma".equals(strategy)) {
            trader = new CrossingMATrader();
        }

        DataStream<String> rawStream = env
                .addSource(new BinanceWebSocketSource(symbol))
                .name("Binance WebSocket Source")
                .uid("binance-websocket-source");

        DataStream<Trade> trades = rawStream
                .map(new TradeMapper())
                .name("JSON -> Trade")
                .uid("trade-mapper");

        DataStream<TradeEvent> events = trades.
                keyBy(Trade::getSymbol).
                process(trader)
                .name("Trading Strategy")
                .uid("trading-strategy");

        if ("db".equals(runningMode)) {
        events.addSink(
                new InfluxBalanceSink(
                        influxUrl,
                        influxToken,
                        influxOrg,
                        influxBucket)
                ).name("InfluxDB Balance Writer")
                .uid("influx-balance-sink");
        } else if ("csv".equals(runningMode)) {
            events.filter(e -> "SELL".equals(e.action))
                    .setParallelism(1)
                    .addSink(new CsvSink())
                    .name("CSV Sell Sink")
                    .uid("csv-sell-sink");

        } else {
            throw new IllegalArgumentException("Unrecognized running mode: " + runningMode);
        }


        try {
            env.execute("Binance BTCUSDT Price Stream");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static class TradeMapper implements MapFunction<String, Trade> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public Trade map(String s) throws Exception {
            return MAPPER.readValue(s, Trade.class);
        }
    }

    private static void validateConfig(ParameterTool params) {
        List<String> required = List.of(
                "INFLUXDB_URL",
                "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN",
                "RUNNING_MODE"
        );

        for (String key : required) {
            if (!params.has(key)) {
                throw new IllegalArgumentException("Missing required parameter: " + key);
            }
        }
    }

}