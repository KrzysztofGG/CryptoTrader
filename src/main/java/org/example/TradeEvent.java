package org.example;

public class TradeEvent {
    public String symbol;
    public double price;
    public double volume;
    public String action;      // BUY / SELL / HOLD
    public String comment;     // e.g., "Fast > Slow", "TAKE PROFIT"
    public double pnl;         // profit/loss (if SELL)
    public double pnlPercent;  // profit/loss %
    public long timestamp;
    public double balance;

    public TradeEvent() {} // default constructor for serialization

    public TradeEvent(String symbol, double price, double volume, String action, String comment,
                      double pnl, double pnlPercent, long timestamp, double balance) {
        this.symbol = symbol;
        this.price = price;
        this.volume = volume;
        this.action = action;
        this.comment = comment;
        this.pnl = pnl;
        this.pnlPercent = pnlPercent;
        this.timestamp = timestamp;
        this.balance = balance;
    }

    @Override
    public String toString() {
        return "TradeEvent{" +
                "symbol='" + symbol + '\'' +
                ", price=" + price +
                ", volume=" + volume +
                ", action='" + action + '\'' +
                ", comment='" + comment + '\'' +
                ", pnl=" + pnl +
                ", pnlPercent=" + pnlPercent +
                ", timestamp=" + timestamp +
                ", balance=" + balance +
                '}';
    }
}
