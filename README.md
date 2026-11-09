# iggy-bench

A reproducible, throughput-first benchmark harness comparing **Apache Iggy** (pinned SDK `0.6.0`, pre-VSR) against **Redpanda** as an architectural peer, driven from a Java client.

This harness is the *scaffolding* for the InfoQ article "Evaluating Apache Iggy: Getting a Coherent Setup Running, and What Thread-Per-Core Streaming Buys You." **It does not ship numbers.** You run it on hardware you control and describe, and the numbers you publish are yours.

---

## Round 3 (current) — what changed and how to run it

Round 3 fixes the two things reviewers flagged about the earlier run: it exercises the
thread-per-core design properly, and it tests the current Iggy version.

**Key changes from Round 2:**

- **Iggy 0.9.0** (was 0.6.0). 0.9.0 uses the VSR wire protocol; `enforce_fsync` was removed
  and durability is now a per-topic option (`replicated` | `persisted`). `IGGY_SYSTEM_*`
  env vars dropped the `SYSTEM_` segment. 0.8.x clients cannot talk to a 0.9.0 server.
- **Dedicated multi-core box** (4–8 dedicated vCPU, not 2 shared), so thread-per-core has
  cores to scale across and the signal isn't buried in shared-vCPU noise.
- **Partition sweep** (1, 4, 8) — the headline result is *scaling vs. parallelism*, not a
  single point. A single partition pins work to ~one shard and barely tests the design.
- **Latency mode** (`--mode latency`): p50/p99/p99.9/p99.99 of single-message round-trips,
  via HdrHistogram with coordinated-omission correction when paced (`--rate`).
- **Per-core CPU capture** (`mpstat -P ALL`) during every run — direct evidence shards are
  active.

### Box prep (Ubuntu, dedicated 4–8 vCPU)

```bash
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 openjdk-21-jdk unzip git sysstat
sudo snap install gradle --classic
# log out/in for docker group, or run docker via sudo
```

Set `--smp` in `docker-compose.yml` (Redpanda) to the box's core count before running.

### Confirming the 0.9.0 Java SDK API (DO THIS FIRST)

The 0.9.0 API drifted from both the docs and the 0.6.0 jar. Every call marked `TODO(0.9.0)`
in `IggyDriver.java` / `SmokeTest.java` must be confirmed against the real jar before the
first run — this is what cost a debugging cycle last time. After a `gradle build` pulls the
jar:

```bash
JAR=$(find ~/.gradle -name "iggy-0.9.0.jar" | head -1)
javap -p -cp "$JAR" org.apache.iggy.client.blocking.tcp.IggyTcpClient
javap -p -cp "$JAR" 'org.apache.iggy.client.blocking.tcp.IggyTcpClient$Builder'
javap -p -cp "$JAR" org.apache.iggy.client.blocking.TopicsClient   # find the durability overload
javap -p -cp "$JAR" org.apache.iggy.client.blocking.MessagesClient
```

Wire the confirmed `createTopic` durability overload (map `DURABLE` → `persisted`) in
`IggyDriver.ensureTopic`. **Do not run the `durable` sweep until that TODO is wired** — until
then Iggy ignores `--durability` and the durable Iggy cells are invalid. The harness warns on
stderr when this happens.

### Run it

```bash
gradle wrapper --gradle-version 8.10.2   # if not already generated
gradle build -x test                     # pulls jars; then do the javap pass above
./gradlew smokeTest                      # must print 10 sent / 10 received

# Full sweep (default durability): 1/4/8 partitions × both engines × throughput+latency, 6 runs each
./run-sweep.sh

# Durable sweep — only after the Iggy durability TODO is wired:
DURABILITY=durable ./run-sweep.sh
```

Results land in `results-r3/*.txt` (one JSON line per run) and per-core CPU in
`results-r3/cpu/*.mpstat`. Discard run 1 of each cell as warm-up; report median + spread.

### New CLI flags

| Flag | Values | Meaning |
| --- | --- | --- |
| `--mode` | `throughput` \| `latency` | bulk msgs/sec, or per-message percentiles |
| `--durability` | `default` \| `durable` | fast (acks before durable) vs synchronous, matched both sides |
| `--partitions` | N | topic partition count (sweep 1/4/8) |
| `--latency-samples` | N | latency mode: timed round-trips to record |
| `--rate` | Hz | latency mode: paced send rate (enables CO correction); 0 = closed-loop |

Everything below this section is the original Round-2 guidance; the version-coherence "trap"
section refers to the 0.6.0-era VSR split and is superseded by "SDK and server must both be
0.9.x" above.

---

## Step 0 — Get these files onto your machine, then into a public GitHub repo

**Where the zip comes from:** `iggy-bench.zip` was generated in the Claude conversation that
created this project. Download it from the file card in that chat (the attachment labelled
**iggy-bench**), save it to wherever you keep projects, then:

```bash
# 1. Unzip
unzip iggy-bench.zip
cd iggy-bench

# 2. Add the two files a public repo should have (see below for contents)
#    - .gitignore
#    - LICENSE  (Apache-2.0, matching the Iggy project)

# 3. Generate the Gradle wrapper so the repo is clone-and-run for anyone.
#    (This project intentionally ships WITHOUT the wrapper binary.)
#    Needs a local gradle: `brew install gradle` or `sdk install gradle`.
gradle wrapper --gradle-version 8.10.2

# 4. Initialize git and make the first commit
git init
git add .
git commit -m "Initial commit: Iggy vs Redpanda throughput harness"

# 5. Create the public repo and push.
#    With GitHub CLI (easiest):
gh repo create iggy-bench --public --source=. --remote=origin --push
#    OR manually — create an empty repo on github.com, then:
#    git remote add origin https://github.com/<you>/iggy-bench.git
#    git branch -M main
#    git push -u origin main
```

**`.gitignore`** (create in the project root):
```
.gradle/
build/
!gradle-wrapper.jar
*.class
.idea/
*.iml
.vscode/
*.log
results/
```

**`LICENSE`** — Apache-2.0. Easiest path: on GitHub, *Add file → Create new file →* name it
`LICENSE` → *Choose a license template → Apache License 2.0*. Or drop the text in locally
before your first commit.

Once this is done, the rest of the steps below are the actual run. Don't publish any numbers
until Step 1 and the smoke test (Step 3) both pass — a repo with mismatched versions produces
results that look real but aren't.

---

## ⚠️ Read this first: the version-coherence trap

Iggy is mid-migration to a new TCP wire protocol called **VSR (Viewstamped Replication)**. This is the single thing most likely to make your setup silently fail or, worse, appear to work while comparing incompatible builds.

The rule, straight from the Apache Iggy Java SDK docs:

> The TCP transport speaks the VSR wire protocol. The SDK and the server must speak the same protocol: the latest published artifact (`0.6.0`) predates VSR and only pairs with servers of the same era, while the SDK on `master` requires a current server.

This harness pins the **pre-VSR** side on purpose, because it is the only combination you can assemble entirely from *published* artifacts (Maven Central + a published server image) without building the server from Rust source. That maximizes reproducibility for readers.

**The one value you must confirm before running** is the Iggy server image tag that pairs with SDK `0.6.0`. It is marked `PIN_ME` in `docker-compose.yml`. See "Step 1" below for exactly how to confirm it. Do not skip this.

---

## What it measures (first profile: high-throughput bulk)

- **Producer throughput**: messages/sec and MB/sec sustained, for a fixed total volume.
- **Consumer drain throughput**: messages/sec read back.
- **End-to-end wall time** for the full produce+consume cycle.

Latency percentiles are intentionally a *second* profile (stubbed, see `LATENCY.md`) so the first article has one clean headline number rather than a muddled one.

All parameters (message size, batch size, message count, partitions) are CLI flags, so you can sweep them and report a curve rather than a single point.

---

## Requirements

- Docker + Docker Compose (you have this locally; the sandbox that generated this file did not, which is why you run it, not the author).
- JDK 21 (the harness targets 17 bytecode; 21 is fine).
- Gradle 8.x. This project ships **without** a `gradlew` wrapper binary. Either use a
  locally installed `gradle`, or generate the wrapper once with:
  ```bash
  gradle wrapper --gradle-version 8.10.2
  ```
  after which `./gradlew` will work. All commands below use `gradle`; swap in `./gradlew`
  if you generated the wrapper.
- ~2 GB free RAM for the two brokers.

---

## Step 1 — Confirm the Iggy server image tag (the PIN_ME)

SDK `0.6.0` is pre-VSR. You need a server image from the *same era*. Confirm the tag before trusting any number:

```bash
# List available Apache Iggy server tags (published to Docker Hub as apache/iggy).
# Pick the newest tag that still predates the VSR cutover. When in doubt, use the
# tag whose release date is closest to — but not after — the 0.6.0 SDK release.
docker run --rm apache/iggy:<candidate-tag> --version   # sanity check it boots

# Then run the smoke test in Step 3. If the Java getting-started example
# round-trips 10 messages, your SDK and server speak the same protocol.
# If you get a protocol/deserialization error on connect, the tags are mismatched:
# that IS the VSR trap. Change the tag, not the SDK.
```

Write the confirmed tag into `docker-compose.yml` where it says `PIN_ME`, and record it in your article's "Reproducing this" box. That single line is what makes your benchmark honest.

> Note: older images were published under `iggyrs/iggy` (e.g. `iggyrs/iggy:0.4.21`) before the move to the `apache/iggy` namespace. If the `apache/iggy` tag you need isn't present, the `iggyrs/iggy` image of the matching era is the fallback. Document whichever you use.

---

## Step 2 — Start the brokers

```bash
docker compose up -d
docker compose ps        # both should be healthy/running
```

Iggy listens on **8090** (TCP). Redpanda listens on **19092** (Kafka API, mapped).

---

## Step 3 — Smoke test (do this before benchmarking)

Proves the pinned SDK actually talks to the pinned server (i.e. you're not in the VSR trap):

```bash
gradle smokeTest
```

This sends 10 messages and reads them back through the Iggy SDK. If it prints 10 sent + 10 received, you're coherent. If it throws on connect, revisit Step 1.

---

## Step 4 — Run the throughput benchmark

```bash
# Iggy
gradle bench --args="--target iggy --messages 1000000 --size 256 --batch 1000 --partitions 1"

# Redpanda (same flags, same driver)
gradle bench --args="--target redpanda --messages 1000000 --size 256 --batch 1000 --partitions 1"
```

Each run prints a compact result block (JSON + human table) you can paste into a spreadsheet. Run each config ≥5 times, discard the first (warm-up), and report median + spread — never a single run.

---

## Honest-benchmark checklist (put this in the article)

- [ ] Pinned versions stated: Iggy SDK `0.6.0`, Iggy server `<tag>`, Redpanda `<tag>`, JDK, OS, hardware.
- [ ] Same driver, same message size, same batch size, same partition count on both sides.
- [ ] Warm-up runs discarded; ≥5 measured runs; median + min/max reported.
- [ ] `fsync`/durability settings stated for BOTH brokers (this is the most common way streaming benchmarks lie — an async-ack broker will always "beat" a synchronous one).
- [ ] Single-node, single-client caveat stated explicitly. This is a client-side evaluation, not a distributed cluster benchmark.
- [ ] What you did NOT measure, said out loud (latency tails, multi-consumer fan-out, failure/recovery, replication).

---

## Layout

```
docker-compose.yml        # both brokers; PIN_ME server tag
build.gradle.kts          # deps: iggy 0.6.0, kafka-clients, HdrHistogram
settings.gradle.kts
src/main/java/org/apache/iggy/bench/
  BenchMain.java          # CLI entrypoint + result reporting
  Target.java             # enum + config
  IggyDriver.java         # produce/consume via Iggy SDK 0.6.0
  RedpandaDriver.java     # produce/consume via kafka-clients
  SmokeTest.java          # 10-message round-trip against Iggy
  Stopwatch.java          # timing + throughput math
LATENCY.md                # how to extend to the p50/p99 profile later
.gitignore
LICENSE                   # Apache-2.0
.github/workflows/build.yml  # compile-only CI (see note below)
```

## Repo housekeeping

- **`.gitignore`, `LICENSE` (Apache-2.0), and CI are already included** in this project, so
  after Step 0 the repo is ready to publish.
- **CI:** `.github/workflows/build.yml` runs a compile-only build on every push — a green
  "it compiles" signal for anyone landing on the repo. It invokes `./gradlew`, so it only
  works once you've generated and committed the wrapper in Step 0
  (`gradle wrapper --gradle-version 8.10.2`). Do **not** run the brokers in CI; benchmarks
  are run locally on hardware you describe.
- Once the smoke test passes, tag a release (e.g. `v0.1`) so the article can cite a stable
  commit.

