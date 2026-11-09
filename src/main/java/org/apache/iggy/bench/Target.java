package org.apache.iggy.bench;

/** Parsed benchmark configuration shared by both drivers. */
public final class Target {

    public enum Which { IGGY, REDPANDA }

    /** What to measure. THROUGHPUT = bulk msgs/sec; LATENCY = per-message p50/p99/p99.9. */
    public enum Mode { THROUGHPUT, LATENCY }

    /**
     * Durability setting, mapped per-engine by the drivers.
     * - DEFAULT: each engine's out-of-the-box behavior (fast, may ack before durable).
     * - DURABLE: synchronous durability on both sides (Iggy persisted / Redpanda fsync).
     * On Iggy 0.9.0 this maps to the topic `durability` option (replicated|persisted);
     * on Redpanda it maps to the --unsafe-bypass-fsync flag (compose-side) + acks.
     */
    public enum Durability { DEFAULT, DURABLE }

    public final Which which;
    public final Mode mode;
    public final Durability durability;
    public final long messages;
    public final int size;        // payload bytes per message
    public final int batch;       // messages per send call
    public final int partitions;  // topic partition count
    public final int latencySamples; // LATENCY mode: how many timed round-trips to record
    public final int rateHz;      // LATENCY mode: target send rate (0 = closed-loop, as fast as acks allow)

    // Connection defaults match docker-compose.yml.
    public final String iggyHost = "127.0.0.1";
    public final int iggyPort = 8090;
    public final String iggyUser = "iggy";
    public final String iggyPass = "iggy";

    public final String redpandaBootstrap = "127.0.0.1:19092";

    private Target(Which which, Mode mode, Durability durability, long messages,
                   int size, int batch, int partitions, int latencySamples, int rateHz) {
        this.which = which;
        this.mode = mode;
        this.durability = durability;
        this.messages = messages;
        this.size = size;
        this.batch = batch;
        this.partitions = partitions;
        this.latencySamples = latencySamples;
        this.rateHz = rateHz;
    }

    public static Target parse(String[] args) {
        Which which = null;
        Mode mode = Mode.THROUGHPUT;
        Durability durability = Durability.DEFAULT;
        long messages = 1_000_000;
        int size = 256;
        int batch = 1000;
        int partitions = 1;
        int latencySamples = 100_000;
        int rateHz = 0;

        // Note: loop runs to args.length (not length-1) so a trailing valueless flag still errors clearly.
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--target" -> which = switch (args[++i].toLowerCase()) {
                    case "iggy" -> Which.IGGY;
                    case "redpanda", "kafka" -> Which.REDPANDA;
                    default -> throw new IllegalArgumentException("unknown --target: " + args[i]);
                };
                case "--mode" -> mode = switch (args[++i].toLowerCase()) {
                    case "throughput", "tput" -> Mode.THROUGHPUT;
                    case "latency", "lat" -> Mode.LATENCY;
                    default -> throw new IllegalArgumentException("unknown --mode: " + args[i]);
                };
                case "--durability" -> durability = switch (args[++i].toLowerCase()) {
                    case "default" -> Durability.DEFAULT;
                    case "durable", "fsync", "persisted" -> Durability.DURABLE;
                    default -> throw new IllegalArgumentException("unknown --durability: " + args[i]);
                };
                case "--messages" -> messages = Long.parseLong(args[++i]);
                case "--size" -> size = Integer.parseInt(args[++i]);
                case "--batch" -> batch = Integer.parseInt(args[++i]);
                case "--partitions" -> partitions = Integer.parseInt(args[++i]);
                case "--latency-samples" -> latencySamples = Integer.parseInt(args[++i]);
                case "--rate" -> rateHz = Integer.parseInt(args[++i]);
                default -> { /* ignore unknown flags for forward-compat */ }
            }
        }
        if (which == null) {
            throw new IllegalArgumentException(
                "usage: --target iggy|redpanda [--mode throughput|latency] [--durability default|durable] " +
                "[--messages N] [--size BYTES] [--batch N] [--partitions N] " +
                "[--latency-samples N] [--rate HZ]");
        }
        return new Target(which, mode, durability, messages, size, batch,
                          partitions, latencySamples, rateHz);
    }

    /** A deterministic payload of the requested size. */
    public byte[] payload() {
        byte[] p = new byte[size];
        // Fill with a repeating, non-zero pattern so compression can't cheat to ~0 bytes.
        for (int i = 0; i < size; i++) {
            p[i] = (byte) ('a' + (i % 26));
        }
        return p;
    }

    @Override public String toString() {
        return "target=" + which + " mode=" + mode + " durability=" + durability
             + " messages=" + messages + " size=" + size + "B batch=" + batch
             + " partitions=" + partitions
             + (mode == Mode.LATENCY ? " latencySamples=" + latencySamples + " rateHz=" + rateHz : "");
    }
}
