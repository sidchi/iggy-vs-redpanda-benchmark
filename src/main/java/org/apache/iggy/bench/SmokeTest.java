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
import java.util.List;
import java.util.Optional;

/**
 * Sends 10 messages and reads them back through the Iggy SDK.
 *
 * PURPOSE: coherence detector. If the pinned SDK and server speak the same wire
 * protocol, this prints 10 sent + 10 received and exits 0. If they don't, you get a
 * protocol/deserialization/connection error here — BEFORE you waste time on a sweep.
 *
 * ┌───────────────────────────────────────────────────────────────────────────┐
 * │ ROUND 3 / 0.9.0: 0.8.x clients cannot talk to a 0.9.0 server. If this fails │
 * │ on connect, the SDK and server tags are on different VSR versions — align   │
 * │ them (both 0.9.x), don't downgrade one. Confirm the builder/login and       │
 * │ createTopic calls below with javap first (see IggyDriver TODO(0.9.0)).      │
 * └───────────────────────────────────────────────────────────────────────────┘
 */
public final class SmokeTest {

    public static void main(String[] args) {
        final String stream = "smoke-stream";
        final String topic = "smoke-topic";
        final StreamId streamId = StreamId.of(stream);
        final TopicId topicId = TopicId.of(topic);

        try {
            // TODO(0.9.0): confirm builder terminal + login against the 0.9.0 jar.
            IggyTcpClient client = IggyTcpClient.builder()
                    .host("127.0.0.1").port(8090)
                    .credentials("iggy", "iggy")
                    .build();
            client.connect();                 // 0.9.0: build() no longer opens the connection
            client.login();     // 0.9.0: direct login() on the client

            if (client.streams().getStream(streamId).isEmpty()) {
                client.streams().createStream(stream);
            }
            if (client.topics().getTopic(streamId, topicId).isEmpty()) {
                // TODO(0.9.0): confirm createTopic signature (durability option replaced enforce_fsync).
                client.topics().createTopic(
                        streamId, 1L, CompressionAlgorithm.None,
                        BigInteger.ZERO, BigInteger.ZERO, topic);
            }

            for (int i = 0; i < 10; i++) {
                client.messages().sendMessages(
                        streamId, topicId,
                        Partitioning.partitionId(0L),
                        List.of(Message.of("smoke-" + i)));
            }
            System.out.println("Sent: 10");

            int received = 0;
            BigInteger offset = BigInteger.ZERO;
            Consumer consumer = Consumer.of(0L);
            while (received < 10) {
                PolledMessages polled = client.messages().pollMessages(
                        streamId, topicId, Optional.of(0L), consumer,
                        PollingStrategy.offset(offset), 10L, false);
                int got = polled.messages().size();
                if (got == 0) break;
                for (Message m : polled.messages()) {
                    System.out.println("  recv: " + new String(m.payload()));
                }
                received += got;
                offset = offset.add(BigInteger.valueOf(got));
            }
            System.out.println("Received: " + received);

            if (received == 10) {
                System.out.println("\n[OK] SDK and server are protocol-coherent. Safe to benchmark.");
            } else {
                System.out.println("\n[WARN] Round-trip incomplete (" + received + "/10).");
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("\n[FAIL] Smoke test failed to connect/round-trip.");
            System.err.println("   Likely a version mismatch: SDK and server must both be 0.9.x (VSR).");
            System.err.println("   Underlying error: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
