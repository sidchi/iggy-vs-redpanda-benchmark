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

public final class IggyDriver {

    private static final String STREAM = "bench-stream";
    private static final String TOPIC = "bench-topic";
    private static final StreamId STREAM_ID = StreamId.of(STREAM);
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC);

    private final Target cfg;

    public IggyDriver(Target cfg) { this.cfg = cfg; }

    private IggyTcpClient connect() {
        IggyTcpClient client = IggyTcpClient.builder()
                .host(cfg.iggyHost)
                .port(cfg.iggyPort)
                .credentials(cfg.iggyUser, cfg.iggyPass)
                .build();
        client.users().login(cfg.iggyUser, cfg.iggyPass);
        return client;
    }

    public double produce() {
        IggyTcpClient client = connect();
        ensureTopic(client);

        String payload = "x".repeat(cfg.size);
        Partitioning partitioning = Partitioning.balanced();

        var sw = new Stopwatch().start();
        long sent = 0;
        while (sent < cfg.messages) {
            int thisBatch = (int) Math.min(cfg.batch, cfg.messages - sent);
            List<Message> batch = new ArrayList<>(thisBatch);
            for (int i = 0; i < thisBatch; i++) batch.add(Message.of(payload));
            client.messages().sendMessages(STREAM_ID, TOPIC_ID, partitioning, batch);
            sent += thisBatch;
        }
        return sw.stop().seconds();
    }

    public double consume() {
        IggyTcpClient client = connect();
        Consumer consumer = Consumer.of(0L);
        var sw = new Stopwatch().start();

        long received = 0;
        for (long p = 0; p < cfg.partitions && received < cfg.messages; p++) {
            BigInteger offset = BigInteger.ZERO;
            while (received < cfg.messages) {
                PolledMessages polled = client.messages().pollMessages(
                        STREAM_ID, TOPIC_ID, Optional.of(p), consumer,
                        PollingStrategy.offset(offset), (long) cfg.batch, false);
                int got = polled.messages().size();
                if (got == 0) break;
                received += got;
                offset = offset.add(BigInteger.valueOf(got));
            }
        }
        double secs = sw.stop().seconds();
        if (received < cfg.messages)
            System.err.printf("WARN: consumed %d of %d messages%n", received, cfg.messages);
        return secs;
    }

    private void ensureTopic(IggyTcpClient client) {
        if (client.streams().getStream(STREAM_ID).isEmpty())
            client.streams().createStream(STREAM);
        if (client.topics().getTopic(STREAM_ID, TOPIC_ID).isEmpty()) {
            client.topics().createTopic(
                    STREAM_ID, (long) cfg.partitions,
                    CompressionAlgorithm.None, BigInteger.ZERO, BigInteger.ZERO,
                    Optional.of((short) 1), TOPIC);
        }
    }
}
