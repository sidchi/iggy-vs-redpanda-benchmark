package org.apache.iggy.bench;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.topic.CompressionAlgorithm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Produce/consume driver for Apache Iggy via the blocking TCP SDK.
 *
 * ┌───────────────────────────────────────────────────────────────────────────┐
 * │ ROUND 3 / 0.9.0 — CONFIRM THESE AGAINST THE REAL JAR WITH javap FIRST.     │
 * │ 0.9.0 moved the TCP client to the VSR wire protocol and the API drifted    │
 * │ from both the docs and the 0.6.0 jar. Every call below marked TODO(0.9.0)  │
 * │ must be verified before the first run. Last time docs-vs-jar drift cost a  │
 * │ debugging cycle; do the javap pass up front. See README "Confirming the    │
 * │ 0.9.0 Java SDK API".                                                        │
 * └───────────────────────────────────────────────────────────────────────────┘
 *
 * API surface as of the 0.6.0 jar (what this file was written against):
 *   IggyTcpClient.builder().host().port().credentials().build()  // then .users().login(u,p)
 *   client.streams().createStream(name)
 *   client.topics().createTopic(streamId, partitions, compression, BigInteger, BigInteger, name)
 *   client.messages().sendMessages(streamId, topicId, partitioning, List<Message>)
 *   client.messages().pollMessages(streamId, topicId, Optional<partition>, consumer, strategy, count, autocommit)
 *
 * 0.9.0 KNOWN CHANGES to re-verify:
 *   - Builder/login: confirm build() vs buildAndLogin(), and whether login is a separate call.
 *   - createTopic: the enforce_fsync topic option was REMOVED in 0.9.0. Durability is now a
 *     topic `durability` option (replicated|persisted). Find the createTopic overload that
 *     accepts the durability option and wire DURABLE -> persisted, DEFAULT -> server default.
 *   - Message.of / payload type (String vs byte[]) — re-confirm.
 *   - pollMessages signature (Optional<Long> partition, etc.) — re-confirm.
 */
public final class IggyDriver {

    private static final String STREAM = "bench-stream";
    private static final String TOPIC = "bench-topic";
    private static final StreamId STREAM_ID = StreamId.of(STREAM);
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC);

    private final Target cfg;

    public IggyDriver(Target cfg) {
        this.cfg = cfg;
    }

    private IggyTcpClient connect() {
        // TODO(0.9.0): confirm builder terminal call + login. On the 0.6.0 jar this was
        // .build() then client.users().login(user, pass). Keep whichever the 0.9.0 jar shows.
        IggyTcpClient client = IggyTcpClient.builder()
                .host(cfg.iggyHost)
                .port(cfg.iggyPort)
                .credentials(cfg.iggyUser, cfg.iggyPass)
                .build();
        client.connect();                              // 0.9.0: open the socket before login
        client.login();      // 0.9.0: direct login() on the client
        return client;
    }

    /** Returns the produce-side elapsed seconds (THROUGHPUT mode). */
    public double produce() {
        try (var client = connect()) {
            ensureTopic(client);

            // TODO(0.9.0): confirm Message.of accepts this type (String vs byte[] on 0.6.0 was String).
            String payload = "x".repeat(cfg.size);
            Partitioning partitioning = Partitioning.balanced(); // spread across partitions

            var sw = new Stopwatch().start();
            long sent = 0;
            while (sent < cfg.messages) {
                int thisBatch = (int) Math.min(cfg.batch, cfg.messages - sent);
                List<Message> batch = new ArrayList<>(thisBatch);
                for (int i = 0; i < thisBatch; i++) {
                    batch.add(Message.of(payload));
                }
                client.messages().sendMessages(STREAM_ID, TOPIC_ID, partitioning, batch);
                sent += thisBatch;
            }
            return sw.stop().seconds();
        }
    }

    /**
     * LATENCY mode: send one message at a time, record send->ack latency per message.
     * Uses batch=1 so each sendMessages call is a single round-trip we can time.
     * Rate pacing (cfg.rateHz > 0) enables coordinated-omission correction in LatencyStats.
     */
    public void produceLatency(LatencyStats stats) {
        try (var client = connect()) {
            ensureTopic(client);
            String payload = "x".repeat(cfg.size);
            Partitioning partitioning = Partitioning.balanced();

            long intervalNanos = cfg.rateHz > 0 ? 1_000_000_000L / cfg.rateHz : 0;
            long start = System.nanoTime();

            for (int i = 0; i < cfg.latencySamples; i++) {
                if (intervalNanos > 0) {
                    long target = start + (long) i * intervalNanos;
                    long now = System.nanoTime();
                    if (now < target) {
                        parkUntil(target);
                    }
                }
                List<Message> one = List.of(Message.of(payload));
                long t0 = System.nanoTime();
                client.messages().sendMessages(STREAM_ID, TOPIC_ID, partitioning, one);
                long t1 = System.nanoTime();
                stats.record(t1 - t0);
            }
        }
    }

    /** Returns the consume-side elapsed seconds for draining `messages` from offset 0. */
    public double consume() {
        try (var client = connect()) {
            Consumer consumer = Consumer.of(0L);
            var sw = new Stopwatch().start();

            long received = 0;
            // Poll per-partition from offset 0 until we've drained the expected count.
            for (long p = 0; p < cfg.partitions && received < cfg.messages; p++) {
                BigInteger offset = BigInteger.ZERO;
                while (received < cfg.messages) {
                    PolledMessages polled = client.messages().pollMessages(
                            STREAM_ID,
                            TOPIC_ID,
                            Optional.of(p),
                            consumer,
                            PollingStrategy.offset(offset),
                            (long) cfg.batch,
                            false);

                    int got = polled.messages().size();
                    if (got == 0) break; // partition drained
                    received += got;
                    offset = offset.add(BigInteger.valueOf(got));
                }
            }
            double secs = sw.stop().seconds();
            if (received < cfg.messages) {
                System.err.printf("WARN: consumed %d of %d messages (partition drain gap)%n",
                        received, cfg.messages);
            }
            return secs;
        }
    }

    private void ensureTopic(IggyTcpClient client) {
        if (client.streams().getStream(STREAM_ID).isEmpty()) {
            client.streams().createStream(STREAM);
        }
        if (client.topics().getTopic(STREAM_ID, TOPIC_ID).isEmpty()) {
            // FINDING (0.9.0): the Durability enum (REPLICATED/PERSISTED) ships in the Java SDK,
            // but NO createTopic or updateTopic overload accepts it (confirmed via javap on
            // iggy-0.9.0.jar). A Java client therefore cannot set per-topic durability in 0.9.0.
            // We create a default-durability topic; the DURABLE sweep is not run for Iggy.
            client.topics().createTopic(
                    STREAM_ID,
                    (long) cfg.partitions,
                    CompressionAlgorithm.None,
                    BigInteger.ZERO,   // message expiry: none
                    BigInteger.ZERO,   // max topic size: unlimited
                    TOPIC);
            if (cfg.durability == Target.Durability.DURABLE) {
                System.err.println("NOTE: Iggy DURABLE requested, but the 0.9.0 Java SDK exposes no "
                        + "createTopic/updateTopic overload that accepts Durability. Running default "
                        + "durability for Iggy. This SDK gap is a documented finding.");
            }
        }
    }

    /** Busy-park to a target nanoTime without oversleeping (fine for sub-ms pacing). */
    private static void parkUntil(long targetNanos) {
        while (System.nanoTime() < targetNanos) {
            java.util.concurrent.locks.LockSupport.parkNanos(targetNanos - System.nanoTime());
        }
    }
}
