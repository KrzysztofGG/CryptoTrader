package org.example.metrics;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.example.model.Trade;


public class Metrics {
    protected final Histogram latencyHistogram;
    protected final Histogram processingHistogram;

    public Metrics(RuntimeContext ctx){
        com.codahale.metrics.Histogram dropwizardLatencyHist =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

        latencyHistogram = ctx
                .getMetricGroup()
                .histogram("event_to_sink_latency_ms", new DropwizardHistogramWrapper(dropwizardLatencyHist));

        com.codahale.metrics.Histogram dropwizardProcessingHist =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

        processingHistogram = ctx
                .getMetricGroup()
                .histogram("processing_latency_ms", new DropwizardHistogramWrapper(dropwizardProcessingHist));

        ctx.getMetricGroup()
                .gauge("event_to_sink_latency_ms_mean", (Gauge<Double>) () -> dropwizardLatencyHist.getSnapshot().getMean());

        ctx.getMetricGroup()
                .gauge("processing_latency_ms_mean", (Gauge<Double>) () -> dropwizardProcessingHist.getSnapshot().getMean());

    }

    public long calculateLatencyMs(Trade trade) {
        long latency = System.currentTimeMillis() - trade.getEventTimeMs();
        latencyHistogram.update(latency);
        return latency;
    }

    public void calculateProcessingMs(long startTime){
        long processingTime = System.currentTimeMillis() - startTime;
        processingHistogram.update(processingTime);
    }
}
