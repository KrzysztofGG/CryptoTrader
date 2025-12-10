package org.example;


import org.apache.flink.api.common.functions.MapFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class Main {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // testing
        env.getConfig().disableClosureCleaner(); // get rid of error

        // Create a DataStream
        DataStream<String> stream = env.addSource(new BinanceWebSocketSource("btcusdt"));
        DataStream<Trade> trades = stream.map(new TradeMapper());

        TradingStrategy trader = new SimpleTrader();

        DataStream<TradeEvent> events = trades.
                keyBy(Trade::getSymbol).
                process(trader)
                .name("Trader");

//        events.map(e -> new ObjectMapper().writeValueAsString(e))
//                .print("TradeEvent");

        // SINK
        final String INFLUX_URL = "http://localhost:8086";
        final String INFLUX_TOKEN = "super-secret-token";
        final String INFLUX_ORG = "trading";
        final String INFLUX_BUCKET = "trading_db";

        events.addSink(new InfluxBalanceSink(INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET))
                .name("InfluxDB Balance Writer");

        try {
            env.execute("Binance BTCUSDT Price Stream");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class TradeMapper implements MapFunction<String, Trade> {
        final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Trade map(String s) throws Exception {
            return mapper.readValue(s, Trade.class);
        }
    }

//    private static void startDockerIfNotRunning(){
//        try {
//            Process check = new ProcessBuilder("bash", "-c",
//                    "docker ps -q --filter name=kafka").start();
//            String result = new String(check.getInputStream().readAllBytes()).trim();
//
//            if (result.isEmpty()) {
//                System.out.println("Kafka not running - starting docker-compose...");
//                new ProcessBuilder("bash", "-c", "docker-compose up -d").inheritIO().start().waitFor();
//                Thread.sleep(5000);
//            } else {
//                System.out.println("Kafka container already running.");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static void waitForKafka(String topicName) {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("request.timeout.ms", "2000");
//        props.put("session.timeout.ms", "2000");
//        while (true) {
//            try (var admin = org.apache.kafka.clients.admin.Admin.create(props)){
//                var topics = admin.listTopics().names().get();
//                if (topics.contains(topicName)) {
//                    System.out.println("Kafka and topic '" + topicName + "' are available");
//                    return;
//                } else {
//                    System.out.println("Kafka is available, waiting for topic: '" + topicName + "'...");
//                }
//            } catch (Exception e) {
//                System.out.println("Waiting for Kafka...");
//            }
//            try {
//                Thread.sleep(2000);
//            } catch (InterruptedException ignored) {}
//        }
//    }
//
//    private static void startKafkaConsumerWindow(String topicName) {
//        new Thread(() -> {
//            try {
//                System.out.println("Starting live Kafka consumer window...");
//                Process p = new ProcessBuilder("bash", "-c",
//                        "docker exec $(docker ps -qf name=kafka) " +
//                        "kafka-console-consumer --bootstrap-server localhost:9092 --topic " + topicName)
//                        .redirectErrorStream(true)
//                        .start();
//                try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
//                    String line;
//                    while ((line = reader.readLine()) != null) {
//                        System.out.println("[Kafka] " + line);
//                    }
//                }
//            } catch (Exception e) {
//                System.out.println("Kafka consumer error: " + e.getMessage());
//            }
//        }).start();
//    }
}