package org.apache.iggy.bench;

/**
 * Benchmark entrypoint. Two modes:
 *
 * THROUGHPUT (bulk msgs/sec + MB/sec):
 *   ./gradlew bench --args="--target iggy     --mode throughput --messages 1000000 --partitions 4"
 *   ./gradlew bench --args="--target redpanda --mode throughput --messages 1000000 --partitions 4"
 *
 * LATENCY (p50/p99/p99.9 of single-message round-trips):
 *   ./gradlew bench --args="--target iggy     --mode latency --latency-samples 100000 --partitions 4 --rate 10000"
 *
 * Prints a human table + ONE JSON line. Run each config >=5x, discard the first (warm-up),
 * report median + spread. This harness deliberately does NOT aggregate runs — aggregating
 * across JVM starts hides variance.
 */
public final class BenchMain {

    public static void main(String[] args) {
        Target cfg = Target.parse(args);
        System.err.println("[config] " + cfg);

        switch (cfg.mode) {
            case THROUGHPUT -> runThroughput(cfg);
            case LATENCY -> runLatency(cfg);
        }
    }

    // ---- THROUGHPUT ----------------------------------------------------------

    private static void runThroughput(Target cfg) {
        double produceSecs;
        double consumeSecs;
        switch (cfg.which) {
            case IGGY -> {
                IggyDriver d = new IggyDriver(cfg);
                produceSecs = d.produce();
                consumeSecs = d.consume();
            }
            case REDPANDA -> {
                RedpandaDriver d = new RedpandaDriver(cfg);
                produceSecs = d.produce();
                consumeSecs = d.consume();
            }
            default -> throw new IllegalStateException();
        }
        reportThroughput(cfg, produceSecs, consumeSecs);
    }

    private static void reportThroughput(Target cfg, double produceSecs, double consumeSecs) {
        double prodMsgs = cfg.messages / produceSecs;
        double prodMB = ((double) cfg.messages * cfg.size / 1_000_000.0) / produceSecs;
        double consMsgs = cfg.messages / consumeSecs;
        double consMB = ((double) cfg.messages * cfg.size / 1_000_000.0) / consumeSecs;

        System.out.println();
        System.out.println("┌─────────────── THROUGHPUT ───────────────┐");
        System.out.printf ("  target        : %s%n", cfg.which);
        System.out.printf ("  durability    : %s%n", cfg.durability);
        System.out.printf ("  partitions    : %d%n", cfg.partitions);
        System.out.printf ("  messages      : %,d  payload %d B  batch %d%n", cfg.messages, cfg.size, cfg.batch);
        System.out.println("  ──────────────────────────────────────────");
        System.out.printf ("  produce       : %.3f s  → %,.0f msg/s  %,.1f MB/s%n", produceSecs, prodMsgs, prodMB);
        System.out.printf ("  consume       : %.3f s  → %,.0f msg/s  %,.1f MB/s%n", consumeSecs, consMsgs, consMB);
        System.out.println("└──────────────────────────────────────────┘");

        System.out.printf(
            "{\"target\":\"%s\",\"mode\":\"throughput\",\"durability\":\"%s\"," +
            "\"messages\":%d,\"size\":%d,\"batch\":%d,\"partitions\":%d," +
            "\"produce_s\":%.4f,\"produce_msgs_s\":%.1f,\"produce_mb_s\":%.2f," +
            "\"consume_s\":%.4f,\"consume_msgs_s\":%.1f,\"consume_mb_s\":%.2f}%n",
            cfg.which, cfg.durability, cfg.messages, cfg.size, cfg.batch, cfg.partitions,
            produceSecs, prodMsgs, prodMB, consumeSecs, consMsgs, consMB);
    }

    // ---- LATENCY -------------------------------------------------------------

    private static void runLatency(Target cfg) {
        long expectedIntervalNanos = cfg.rateHz > 0 ? 1_000_000_000L / cfg.rateHz : 0;
        LatencyStats stats = new LatencyStats(expectedIntervalNanos);

        switch (cfg.which) {
            case IGGY -> new IggyDriver(cfg).produceLatency(stats);
            case REDPANDA -> new RedpandaDriver(cfg).produceLatency(stats);
            default -> throw new IllegalStateException();
        }
        reportLatency(cfg, stats);
    }

    private static void reportLatency(Target cfg, LatencyStats stats) {
        System.out.println();
        System.out.println("┌─────────────── LATENCY ───────────────┐");
        System.out.printf ("  target        : %s%n", cfg.which);
        System.out.printf ("  durability    : %s%n", cfg.durability);
        System.out.printf ("  partitions    : %d  payload %d B  rate %s%n",
                cfg.partitions, cfg.size, cfg.rateHz > 0 ? cfg.rateHz + " Hz" : "closed-loop");
        System.out.println("  ───────────────────────────────────────");
        stats.printTable();
        System.out.println("└───────────────────────────────────────┘");

        System.out.printf(
            "{\"target\":\"%s\",\"mode\":\"latency\",\"durability\":\"%s\"," +
            "\"size\":%d,\"partitions\":%d,\"rate_hz\":%d,%s}%n",
            cfg.which, cfg.durability, cfg.size, cfg.partitions, cfg.rateHz,
            stats.toJsonFields());
    }
}
