package org.example;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SimpleTrader extends TradingStrategy {

    private transient ListState<Double> lastNPrices;
    private final double threshold = 0.0001; // 0.1% divergence
    private final int N = 5;

    @Override
    public void open(Configuration parameters) {
        super.open(parameters);
        lastNPrices = getRuntimeContext().getListState(
                new ListStateDescriptor<>("lastNPrices", Double.class));
    }

    @Override
    public void processElement(
            Trade trade,
            KeyedProcessFunction<String, Trade, TradeEvent>.Context context,
            Collector<TradeEvent> collector) throws Exception {

        // Handle obtaining moving average
        List<Double> prices = StreamSupport.stream(lastNPrices.get().spliterator(), false)
                        .collect(Collectors.toList());

        prices.add(trade.getClosePrice());

        if (prices.size() > N)
            prices = prices.subList(prices.size() - N, prices.size());

        lastNPrices.update(prices);

        double movingAvg = movingAverage(prices);

        // Trading logic
        Double entry = entryPrice.value();

        // ---- ENTRY LOGIC ----
        if (entry == null && trade.getClosePrice() < movingAvg * (1 - threshold)) {
            buy(trade, collector);

        // ---- EXIT LOGIC ----
        } else if (entry != null && trade.getClosePrice() > movingAvg * (1 + threshold)) {
            sell(trade, "Value over MA", collector);
        }

        // ---- NO ACTION ----
        else {

            hold(trade, collector);
        }
    }

}
