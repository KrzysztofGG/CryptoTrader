package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class Trade {
    @JsonProperty("e")
    public String eventType;

    @JsonProperty("E")
    public long eventTimeMs;

    @JsonProperty("s")
    public String symbol;

    @JsonProperty("c")
    public double closePrice;

    @JsonProperty("o")
    public double openPrice;

    @JsonProperty("h")
    public double highPrice;

    @JsonProperty("l")
    public double lowPrice;

    @JsonProperty("v")
    public double volume;

    @JsonProperty("q")
    public double totalVolume;

    public Date getEventTime() {
        return new Date(eventTimeMs);
    }

    public String getSymbol() {
        return symbol;
    }

    public double getClosePrice() {
        return closePrice;
    }

    public double getVolume() {
        return volume;
    }

    @Override
    public String toString() {
        return "Trade{" +
                "eventType='" + eventType + '\'' +
                ", eventTime=" + getEventTime() +
                ", symbol='" + symbol + '\'' +
                ", closePrice=" + closePrice +
                ", openPrice=" + openPrice +
                ", highPrice=" + highPrice +
                ", lowPrice=" + lowPrice +
                ", volume=" + volume +
                ", totalVolume=" + totalVolume +
                '}';
    }
}
