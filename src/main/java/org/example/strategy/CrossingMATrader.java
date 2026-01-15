package org.example.strategy;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.model.Trade;
import org.example.model.TradeEvent;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CrossingMATrader extends TradingStrategy {

    private final int FAST = 5;   // short-term MA
    private final int SLOW = 18;  // longer-term MA
    private final double stopLossThreshold = 0.01;
    private final double takeProfitThreshold = 0.02;
    private final double volumeThreshold = 0.005;
    private final boolean checkVolume = false;

    private transient ValueState<Double> prevFastMA;
    private transient ValueState<Double> prevSlowMA;
    private transient ListState<Double> priceHistory;
    private transient ListState<Double> volumeHistory;

    @Override
    public void open(Configuration parameters) {
        super.open(parameters);
        prevFastMA = getRuntimeContext().getState(
                new ValueStateDescriptor<>("prevFastMA", Double.class));
        prevSlowMA = getRuntimeContext().getState(
                new ValueStateDescriptor<>("prevSlowMA", Double.class));
        priceHistory = getRuntimeContext().getListState(
                new ListStateDescriptor<>("priceHistory", Double.class));
        volumeHistory = getRuntimeContext().getListState(
                new ListStateDescriptor<>("volumeHistory", Double.class));
    }

    @Override
    public void processElement(
            Trade trade,
            KeyedProcessFunction<String, Trade, TradeEvent>.Context context,
            Collector<TradeEvent> collector) throws Exception {

        long startTime = System.currentTimeMillis();

        if (cashBalance.value() == null) cashBalance.update(initialCapital);

        // Handle keeping prices up to date
        List<Double> prices = updateHistory(priceHistory, trade.getClosePrice(), SLOW);
        if (prices.size() < SLOW) return;

        // Handle keeping volumes up to date
        List<Double> volumes = updateHistory(volumeHistory, trade.getVolume(), SLOW);
        if (volumes.size() < SLOW) return;

        // Count moving averages
        double fastMA = movingAverage(prices.subList(prices.size() - FAST, prices.size()));
        double slowMA = movingAverage(prices);
        double volumeMA = movingAverage(volumes);

        // Check if crosses happen
        Double lastFast = prevFastMA.value();
        Double lastSlow = prevSlowMA.value();

        boolean bullishCross = false;
        boolean bearishCross = false;

        if (lastFast != null && lastSlow != null) {
            bullishCross = (lastFast < lastSlow) && (fastMA > slowMA);
            bearishCross = (lastFast > lastSlow) && (fastMA < slowMA);
        }



        Double entry = entryPrice.value();
        double price = trade.getClosePrice();

        // Handle take profit and stop loss
        if (entry != null) {

            double takeProfitPrice = entry * (1 + takeProfitThreshold);
            double stopLossPrice   = entry * (1 - stopLossThreshold);

            if (price >= takeProfitPrice) {
                sell(trade, "TAKE PROFIT", collector);
                return;

            } else if (price <= stopLossPrice) {
                sell(trade, "STOP LOSS", collector);
                return;
            }

            if (bearishCross) {
                sell(trade, "BEARISH CROSS", collector);
                return;
            }

        } else {
            // Buy when fast MA crosses slow MA
            boolean volumeOK = !checkVolume || trade.getVolume() >= volumeMA * (1 + volumeThreshold);
            if (bullishCross && volumeOK) {
                buy(trade, collector);
            } else{
                LOG.info(
                        "price={}, fastMA={}, slowMA={}, volumeOK={}, bullishCross={}",
                        price, fastMA, slowMA, volumeOK, bullishCross
                );
                hold(trade, collector);
            }
        }

        // store MA history for next tick
        prevFastMA.update(fastMA);
        prevSlowMA.update(slowMA);

        metrics.calculateProcessingMs(startTime);
        metrics.calculateLatencyMs(trade);

    }

    private List<Double> updateHistory(ListState<Double> state, double newValue, int max) throws Exception {
        List<Double> list = StreamSupport.stream(state.get().spliterator(), false)
                .collect(Collectors.toList());

        list.add(newValue);

        if (list.size() > max) {
            list = list.subList(list.size() - max, list.size());
        }

        state.update(list);
        return list;
    }

}
