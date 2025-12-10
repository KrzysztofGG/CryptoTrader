package org.example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;

public class BinanceWebSocketSource implements SourceFunction<String> {

    private volatile boolean running = true;
    private WebSocketClient client;

    private final String symbol;

    public BinanceWebSocketSource(String symbol) {
        this.symbol = symbol.toLowerCase();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        String url = "wss://stream.binance.com:9443/ws/" + symbol + "@miniTicker";

        client = new WebSocketClient(new URI(url)) {
            @Override
            public void onOpen(ServerHandshake serverHandshake) {
                System.out.println("Connected to Binance WebSocket for " + symbol);
            }

            @Override
            public void onMessage(String message) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(message);
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                System.out.println("WebSocket closed: " + reason);
            }

            @Override
            public void onError(Exception e) {
                e.printStackTrace();
            }
        };

        client.connectBlocking(); // wait for connection

        // keep running
        while (running) {
            Thread.sleep(100);
        }

    }

    @Override
    public void cancel() {
        running = false;
        if (client != null) client.close();
    }
}
