package org.apache.iggy.bench;

/** Parsed benchmark configuration shared by both drivers. */
public final class Target {

    public enum Which { IGGY, REDPANDA }

    public final Which which;
    public final long messages;
    public final int size;        // payload bytes per message
    public final int batch;       // messages per send call
    public final int partitions;  // topic partition count

    // Connection defaults match docker-compose.yml.
    public final String iggyHost = "127.0.0.1";
    public final int iggyPort = 8090;
    public final String iggyUser = "iggy";
    public final String iggyPass = "iggy";

    public final String redpandaBootstrap = "127.0.0.1:19092";

    private Target(Which which, long messages, int size, int batch, int partitions) {
        this.which = which;
        this.messages = messages;
        this.size = size;
        this.batch = batch;
        this.partitions = partitions;
    }

    public static Target parse(String[] args) {
        Which which = null;
        long messages = 1_000_000;
        int size = 256;
        int batch = 1000;
        int partitions = 1;

        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--target" -> which = switch (args[++i].toLowerCase()) {
                    case "iggy" -> Which.IGGY;
                    case "redpanda", "kafka" -> Which.REDPANDA;
                    default -> throw new IllegalArgumentException("unknown --target: " + args[i]);
                };
                case "--messages" -> messages = Long.parseLong(args[++i]);
                case "--size" -> size = Integer.parseInt(args[++i]);
                case "--batch" -> batch = Integer.parseInt(args[++i]);
                case "--partitions" -> partitions = Integer.parseInt(args[++i]);
                default -> { /* ignore unknown flags for forward-compat */ }
            }
        }
        if (which == null) {
            throw new IllegalArgumentException(
                "usage: --target iggy|redpanda [--messages N] [--size BYTES] [--batch N] [--partitions N]");
        }
        return new Target(which, messages, size, batch, partitions);
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
        return "target=" + which + " messages=" + messages + " size=" + size
             + "B batch=" + batch + " partitions=" + partitions;
    }
}
