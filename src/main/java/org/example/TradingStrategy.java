package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.List;

public abstract class TradingStrategy
        extends KeyedProcessFunction<String, Trade, TradeEvent> {

    protected transient ValueState<Double> cashBalance;
    protected transient ValueState<Double> entryPrice;
    protected transient ValueState<Double> positionVolume ;
    protected final double initialCapital = 100_000;
    protected final double investmentSize = 0.20; //invest 20%

    @Override
    public void open(Configuration parameters) {
        cashBalance = getRuntimeContext().getState(new ValueStateDescriptor<>("cashBalance", Double.class));
        entryPrice = getRuntimeContext().getState(new ValueStateDescriptor<>("entryPrice", Double.class));
        positionVolume  = getRuntimeContext().getState(new ValueStateDescriptor<>("volume", Double.class));
    }

    protected double movingAverage(List<Double> prices) {
        return prices.stream().mapToDouble(x -> x).average().orElse(0.0);
    }

    protected void buy(Trade trade, Collector<TradeEvent> out) throws Exception {

        Double balance = cashBalance.value();
        if (balance == null) {
            balance = initialCapital;
            cashBalance.update(balance);
        }

        double volume = (balance * investmentSize) / trade.getClosePrice();

        cashBalance.update(balance - volume * trade.getClosePrice());
        entryPrice.update(trade.getClosePrice());
        positionVolume .update(volume);

        System.out.println("BUY " + trade.getSymbol() + " PRICE: " + trade.getClosePrice() + " VOLUME: " + volume);

        out.collect(new TradeEvent(
                trade.getSymbol(),
                trade.getClosePrice(),
                volume,
                "BUY",
                "Entry executed",
                0,
                0,
                System.currentTimeMillis(),
                cashBalance.value()
        ));
    }

    protected void sell(Trade trade, String reason, Collector<TradeEvent> out) throws Exception {
        double entry = entryPrice.value();
        double volume = positionVolume.value();

        double pnl = (trade.getClosePrice() - entry) * volume;

        cashBalance.update(cashBalance.value() + trade.getClosePrice() * volume);
        entryPrice.clear();
        positionVolume.clear();

        System.out.println("SELL " + trade.getSymbol() + " PnL: " + pnl + "$" + " BALANCE: " + cashBalance.value());

        out.collect(new TradeEvent(
                trade.getSymbol(),
                trade.getClosePrice(),
                volume,
                "SELL",
                reason,
                pnl,
                pnl / (entry * volume),
                System.currentTimeMillis(),
                cashBalance.value()
        ));
    }

    protected void hold(Trade trade, Collector<TradeEvent> out) throws Exception {

        Double balance = cashBalance.value();
        if (balance == null) {
            balance = initialCapital;
            cashBalance.update(balance);
        }
        out.collect(new TradeEvent(
                trade.getSymbol(),
                0,
                0,
                "HOLD",
                "",
                0,
                0,
                System.currentTimeMillis(),
                balance
        ));
    }
}