package org.apache.iggy.bench;

/**
 * Throughput benchmark entrypoint.
 *
 *   ./gradlew bench --args="--target iggy     --messages 1000000 --size 256 --batch 1000 --partitions 1"
 *   ./gradlew bench --args="--target redpanda --messages 1000000 --size 256 --batch 1000 --partitions 1"
 *
 * Prints a human table + a JSON line. Run >=5x, discard the first (warm-up),
 * report median + min/max. This harness deliberately does NOT aggregate runs for
 * you — aggregating across JVM starts keeps each run's numbers honest and lets you
 * see variance rather than hiding it behind an average.
 */
public final class BenchMain {

    public static void main(String[] args) {
        Target cfg = Target.parse(args);
        System.err.println("[config] " + cfg);

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

        report(cfg, produceSecs, consumeSecs);
    }

    private static void report(Target cfg, double produceSecs, double consumeSecs) {
        double prodMsgs = cfg.messages / produceSecs;
        double prodMB = ((double) cfg.messages * cfg.size / 1_000_000.0) / produceSecs;
        double consMsgs = cfg.messages / consumeSecs;
        double consMB = ((double) cfg.messages * cfg.size / 1_000_000.0) / consumeSecs;

        System.out.println();
        System.out.println("┌─────────────── RESULT ───────────────┐");
        System.out.printf ("  target        : %s%n", cfg.which);
        System.out.printf ("  messages      : %,d%n", cfg.messages);
        System.out.printf ("  payload       : %d B%n", cfg.size);
        System.out.printf ("  batch         : %d%n", cfg.batch);
        System.out.printf ("  partitions    : %d%n", cfg.partitions);
        System.out.println("  ─────────────────────────────────────");
        System.out.printf ("  produce       : %.3f s%n", produceSecs);
        System.out.printf ("    → msgs/sec  : %,.0f%n", prodMsgs);
        System.out.printf ("    → MB/sec    : %,.1f%n", prodMB);
        System.out.printf ("  consume       : %.3f s%n", consumeSecs);
        System.out.printf ("    → msgs/sec  : %,.0f%n", consMsgs);
        System.out.printf ("    → MB/sec    : %,.1f%n", consMB);
        System.out.println("└──────────────────────────────────────┘");

        // Machine-readable line for spreadsheet ingestion.
        System.out.printf(
            "{\"target\":\"%s\",\"messages\":%d,\"size\":%d,\"batch\":%d,\"partitions\":%d," +
            "\"produce_s\":%.4f,\"produce_msgs_s\":%.1f,\"produce_mb_s\":%.2f," +
            "\"consume_s\":%.4f,\"consume_msgs_s\":%.1f,\"consume_mb_s\":%.2f}%n",
            cfg.which, cfg.messages, cfg.size, cfg.batch, cfg.partitions,
            produceSecs, prodMsgs, prodMB, consumeSecs, consMsgs, consMB);
    }
}
