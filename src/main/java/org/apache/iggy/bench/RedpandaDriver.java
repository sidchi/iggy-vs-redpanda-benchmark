package org.apache.iggy.bench;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Redpanda driver via the standard Kafka client. Redpanda is Kafka-API compatible,
 * so this is plain kafka-clients pointed at the Redpanda broker.
 *
 * DURABILITY NOTE (state this in the article): acks are chosen to match Iggy's setting.
 * - cfg.durability == DEFAULT: acks=1 (leader ack, fast), matching Iggy's default.
 * - cfg.durability == DURABLE: acks=all, AND the Redpanda broker must run WITHOUT
 *   --unsafe-bypass-fsync (handled compose-side). An async-ack broker always "wins" a
 *   throughput race against a synchronous one — that's a misconfiguration, not a finding,
 *   so keep both sides matched and say so.
 */
public final class RedpandaDriver {

    private static final String TOPIC = "bench-topic";
    private final Target cfg;

    public RedpandaDriver(Target cfg) {
        this.cfg = cfg;
    }

    private Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.redpandaBootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Durability-matched acks: all for DURABLE, 1 for DEFAULT.
        props.put(ProducerConfig.ACKS_CONFIG,
                cfg.durability == Target.Durability.DURABLE ? "all" : "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 256 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none"); // fair match to Iggy None
        // Low-latency hygiene so the tail reflects the broker, not Kafka-client timers:
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "300000");   // no periodic metadata refresh mid-run
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.put("max.block.ms", "30000");
        return props;
    }

    /** THROUGHPUT mode: bulk produce, returns elapsed seconds. */
    public double produce() {
        byte[] payload = cfg.payload();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps())) {
            var sw = new Stopwatch().start();
            long sent = 0;
            while (sent < cfg.messages) {
                int thisBatch = (int) Math.min(cfg.batch, cfg.messages - sent);
                for (int i = 0; i < thisBatch; i++) {
                    producer.send(new ProducerRecord<>(TOPIC, null, payload));
                }
                sent += thisBatch;
            }
            producer.flush(); // ensure everything is acked before we stop the clock
            return sw.stop().seconds();
        }
    }

    /**
     * LATENCY mode: send one record at a time, block on its ack future, record the time.
     * linger is set to 0 here so a single record is not held waiting to batch.
     * Rate pacing (cfg.rateHz > 0) enables coordinated-omission correction in LatencyStats.
     */
    public void produceLatency(LatencyStats stats) {
        Properties props = producerProps();
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0"); // no batching delay for single sends
        byte[] payload = cfg.payload();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            // WARMUP: force metadata fetch + connection establishment OUTSIDE the timed loop,
            // otherwise the first few sends carry a multi-hundred-ms metadata stall into the tail.
            try {
                producer.send(new ProducerRecord<>(TOPIC, null, payload)).get();
            } catch (Exception e) {
                throw new RuntimeException("warmup send failed", e);
            }
            producer.flush();

            long intervalNanos = cfg.rateHz > 0 ? 1_000_000_000L / cfg.rateHz : 0;
            long start = System.nanoTime();

            for (int i = 0; i < cfg.latencySamples; i++) {
                if (intervalNanos > 0) {
                    long target = start + (long) i * intervalNanos;
                    if (System.nanoTime() < target) parkUntil(target);
                }
                long t0 = System.nanoTime();
                try {
                    producer.send(new ProducerRecord<>(TOPIC, null, payload)).get();
                } catch (Exception e) {
                    throw new RuntimeException("Kafka send failed at sample " + i, e);
                }
                long t1 = System.nanoTime();
                stats.record(t1 - t0);
            }
        }
    }

    /** THROUGHPUT mode: drain `messages` from the beginning, returns elapsed seconds. */
    public double consume() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.redpandaBootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bench-" + System.nanoTime());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.max(500, cfg.batch));

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (int p = 0; p < cfg.partitions; p++) {
                tps.add(new TopicPartition(TOPIC, p));
            }
            consumer.assign(tps);
            consumer.seekToBeginning(tps);

            var sw = new Stopwatch().start();
            long received = 0;
            while (received < cfg.messages) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    break; // guard against an infinite loop if fewer messages exist than expected
                }
                received += records.count();
            }
            double secs = sw.stop().seconds();
            if (received < cfg.messages) {
                System.err.printf("WARN: consumed %d of %d messages%n", received, cfg.messages);
            }
            return secs;
        }
    }

    private static void parkUntil(long targetNanos) {
        while (System.nanoTime() < targetNanos) {
            java.util.concurrent.locks.LockSupport.parkNanos(targetNanos - System.nanoTime());
        }
    }
}
