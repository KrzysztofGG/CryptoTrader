package org.example.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.model.TradeEvent;

import java.io.IOException;
import java.util.List;

public class CsvSink extends RichSinkFunction<TradeEvent> {

    private transient WriteCSV csv;

    @Override
    public void open(Configuration parameters) throws Exception {
        String outputPath = System.getenv("OUTPUT_FILE_PATH");

        if (outputPath == null) {
            throw new IllegalStateException("OUTPUT_FILE_PATH not set");
        }

        csv = new WriteCSV(
                outputPath,
                List.of("timestamp",
                        "balance",
                        "pnl",
                        "pnlPercent",
                        "symbol")
        );
    }

    @Override
    public void invoke(TradeEvent event, Context context) throws IOException {
        if (!"SELL".equals(event.action)) {
            return;
        }

        csv.writeRow(List.of(
                event.timestamp,
                event.balance,
                event.pnl,
                event.pnlPercent,
                event.symbol
        ));
    }
}
