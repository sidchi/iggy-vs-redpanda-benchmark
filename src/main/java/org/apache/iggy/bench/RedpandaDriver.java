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
 * DURABILITY NOTE (state this in the article): acks and linger settings below are
 * chosen to be a *fair* match to Iggy's default acknowledgement behavior. If you
 * change Iggy's durability, change these to match, and say so. An async-ack broker
 * always "wins" a throughput race against a synchronous one — that's not a finding,
 * it's a misconfiguration.
 */
public final class RedpandaDriver {

    private static final String TOPIC = "bench-topic";
    private final Target cfg;

    public RedpandaDriver(Target cfg) {
        this.cfg = cfg;
    }

    public double produce() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.redpandaBootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");          // fair match — document this
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");     // allow batching
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 256 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none"); // fair match to Iggy None

        byte[] payload = cfg.payload();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
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
                    // Guard against an infinite loop if fewer messages exist than expected.
                    break;
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
}
