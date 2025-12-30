package org.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class BinanceWebSocketSource implements SourceFunction<String> {

    private static final Logger LOG =
            LoggerFactory.getLogger(BinanceWebSocketSource.class);

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
                LOG.info("Connected to Binance WebSocket for " + symbol);
            }

            @Override
            public void onMessage(String message) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(message);
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                LOG.info("WebSocket closed: " + reason);
            }

            @Override
            public void onError(Exception e) {
                LOG.error(e.getMessage());
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
