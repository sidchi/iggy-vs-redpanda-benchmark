package org.apache.iggy.bench;

import org.HdrHistogram.Histogram;

/**
 * Records per-message latencies and reports percentiles.
 *
 * Coordinated omission: if the harness paces at a fixed rate (--rate), a stall hides
 * behind the samples it delays, and the tail lies. When an expected interval is given
 * we use HdrHistogram's recordValueWithExpectedInterval, which back-fills the missing
 * samples a stall should have produced. In closed-loop mode (rate 0) there is no fixed
 * schedule to violate, so we record raw values and SAY SO in the output.
 *
 * Values are stored in microseconds (3 significant digits, up to ~60 s).
 */
public final class LatencyStats {

    private final Histogram h = new Histogram(60_000_000L, 3); // up to 60s in micros
    private final long expectedIntervalMicros; // 0 => closed-loop, no CO correction
    private long count;

    /** expectedIntervalNanos = 0 for closed-loop (no coordinated-omission correction). */
    public LatencyStats(long expectedIntervalNanos) {
        this.expectedIntervalMicros = expectedIntervalNanos / 1_000;
    }

    public void record(long latencyNanos) {
        long micros = latencyNanos / 1_000;
        if (micros < 1) micros = 1;
        if (expectedIntervalMicros > 0) {
            h.recordValueWithExpectedInterval(micros, expectedIntervalMicros);
        } else {
            h.recordValue(micros);
        }
        count++;
    }

    public long samples()            { return count; }
    public double p(double pct)      { return h.getValueAtPercentile(pct) / 1000.0; } // ms
    public double mean()             { return h.getMean() / 1000.0; }                  // ms
    public double max()              { return h.getMaxValue() / 1000.0; }              // ms
    public boolean coCorrected()     { return expectedIntervalMicros > 0; }

    /** JSON fragment of the latency fields (all milliseconds). */
    public String toJsonFields() {
        return String.format(
            "\"lat_samples\":%d,\"co_corrected\":%b," +
            "\"p50_ms\":%.3f,\"p90_ms\":%.3f,\"p99_ms\":%.3f,\"p999_ms\":%.3f," +
            "\"p9999_ms\":%.3f,\"max_ms\":%.3f,\"mean_ms\":%.3f",
            count, coCorrected(),
            p(50), p(90), p(99), p(99.9), p(99.99), max(), mean());
    }

    public void printTable() {
        System.out.printf("  samples       : %,d%s%n", count,
                coCorrected() ? " (coordinated-omission corrected)" : " (closed-loop, raw)");
        System.out.printf("  p50           : %.3f ms%n", p(50));
        System.out.printf("  p90           : %.3f ms%n", p(90));
        System.out.printf("  p99           : %.3f ms%n", p(99));
        System.out.printf("  p99.9         : %.3f ms%n", p(99.9));
        System.out.printf("  p99.99        : %.3f ms%n", p(99.99));
        System.out.printf("  max           : %.3f ms%n", max());
        System.out.printf("  mean          : %.3f ms%n", mean());
    }
}
