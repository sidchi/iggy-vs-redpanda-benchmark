# Second profile: latency (p50/p99/p999)

The throughput harness answers "how much." A latency profile answers "how consistently,"
which is where thread-per-core architectures like Iggy and Redpanda often tell the more
interesting story — the tail, not the mean.

Deliberately kept as a **separate** profile so the first article has one clean headline
number. Add this as a follow-up section (or a second article) rather than muddying the
throughput result.

## Design

- Send one message, record send→ack (or send→poll-visible) latency, repeat.
- Record into an `HdrHistogram` (`org.hdrhistogram:HdrHistogram`, already a dependency).
- Report p50 / p90 / p99 / p99.9 / max, plus the coordinated-omission-corrected variant.

## The one trap to avoid: coordinated omission

If you send, wait for the reply, then send again, a single stall hides behind the next
sample and your p99 lies. Use `Histogram.recordValueWithExpectedInterval(latency, interval)`
or drive at a fixed rate with a scheduled sender. State which you did.

```java
import org.HdrHistogram.Histogram;

Histogram h = new Histogram(3);                 // 3 significant digits
long expectedIntervalNanos = 1_000_000;          // if pacing at 1kHz
// per sample:
h.recordValueWithExpectedInterval(latencyNanos, expectedIntervalNanos);
// at the end:
h.outputPercentileDistribution(System.out, 1000.0);  // scale ns→us
```

## Fairness notes (same as throughput)

- Same payload size, same durability/ack settings on both brokers.
- Warm up the JIT before recording (throw away the first N thousand samples).
- Single in-flight request for a pure latency read; note that this caps throughput
  and is a different measurement from the bulk profile.
