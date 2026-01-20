package org.example.metrics;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.example.model.Trade;


public class Metrics {
    protected final Histogram latencyHistogram;
    protected final Histogram processingHistogram;
    private final Counter totalTrades;
    private final Counter winningTrades;

    public Metrics(RuntimeContext ctx){

        MetricGroup metricGroup = ctx.getMetricGroup();

        com.codahale.metrics.Histogram dropwizardLatencyHist =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

        latencyHistogram = metricGroup
                .histogram("event_to_sink_latency_ms", new DropwizardHistogramWrapper(dropwizardLatencyHist));

        com.codahale.metrics.Histogram dropwizardProcessingHist =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

        processingHistogram = metricGroup
                .histogram("processing_latency_ms", new DropwizardHistogramWrapper(dropwizardProcessingHist));

        metricGroup
                .gauge("event_to_sink_latency_ms_mean", (Gauge<Double>) () -> dropwizardLatencyHist.getSnapshot().getMean());

        metricGroup
                .gauge("processing_latency_ms_mean", (Gauge<Double>) () -> dropwizardProcessingHist.getSnapshot().getMean());

        totalTrades = metricGroup.counter("trades_total");
        winningTrades = metricGroup.counter("trades_winning");

        metricGroup.gauge("hit_rate_percent", () -> {
            long total = totalTrades.getCount();
            return total == 0 ? 0.0 : (winningTrades.getCount() * 100.0) / total;
        });
    }

    public void markSell(boolean profit) {
        totalTrades.inc();
        if (profit) {
            winningTrades.inc();
        }
    }

    public void calculateLatencyMs(Trade trade) {
        long latency = System.currentTimeMillis() - trade.getEventTimeMs();
        latencyHistogram.update(latency);
    }

    public void calculateProcessingMs(long startTime){
        long processingTime = System.currentTimeMillis() - startTime;
        processingHistogram.update(processingTime);
    }
}
