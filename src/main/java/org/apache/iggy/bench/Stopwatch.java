package org.apache.iggy.bench;

/** Minimal nanosecond stopwatch plus throughput math. No external deps. */
public final class Stopwatch {
    private long startNanos;
    private long elapsedNanos = -1;

    public Stopwatch start() {
        startNanos = System.nanoTime();
        elapsedNanos = -1;
        return this;
    }

    public Stopwatch stop() {
        elapsedNanos = System.nanoTime() - startNanos;
        return this;
    }

    public double seconds() {
        if (elapsedNanos < 0) throw new IllegalStateException("stopwatch not stopped");
        return elapsedNanos / 1_000_000_000.0;
    }

    /** messages per second over the measured window. */
    public double msgsPerSec(long messages) {
        return messages / seconds();
    }

    /** megabytes per second, where 1 MB = 1_000_000 bytes (decimal, stated in article). */
    public double mbPerSec(long messages, int messageSizeBytes) {
        double totalBytes = (double) messages * messageSizeBytes;
        return (totalBytes / 1_000_000.0) / seconds();
    }
}
