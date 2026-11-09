plugins {
    java
    application
}

repositories {
    mavenCentral()
}

java {
    toolchain {
        // Round 3 targets JDK 21 (what we install on the box). 17 bytecode is fine too;
        // 21 is set because that is what the droplet has and the toolchain must match.
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    // Apache Iggy Java SDK — Round 3 pins the current 0.9.0 release (VSR wire protocol).
    // Coordinates are org.apache.iggy:iggy. 0.8.x clients CANNOT talk to a 0.9.0 server,
    // so the server image and this SDK must both be 0.9.x. Confirm the exact 0.9.0 API
    // with `javap` on the box before trusting IggyDriver (see its TODO markers).
    implementation("org.apache.iggy:iggy:0.9.0")

    // Kafka protocol client — drives the Redpanda side.
    implementation("org.apache.kafka:kafka-clients:3.8.0")

    // Latency histograms — the LATENCY mode uses these (coordinated-omission correction).
    implementation("org.hdrhistogram:HdrHistogram:2.2.2")

    // Minimal logging so SDK/Kafka noise doesn't pollute result output.
    implementation("org.slf4j:slf4j-simple:2.0.13")
}

application {
    mainClass = "org.apache.iggy.bench.BenchMain"
}

// Throughput:  ./gradlew bench --args="--target iggy --mode throughput --messages 1000000 --partitions 4"
// Latency:     ./gradlew bench --args="--target iggy --mode latency --latency-samples 100000 --partitions 4"
tasks.register<JavaExec>("bench") {
    group = "benchmark"
    description = "Run the benchmark against --target iggy|redpanda (--mode throughput|latency)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "org.apache.iggy.bench.BenchMain"
}

// ./gradlew smokeTest  — proves the pinned SDK <-> server are protocol-coherent (0.9.0 VSR)
tasks.register<JavaExec>("smokeTest") {
    group = "verification"
    description = "10-message round-trip against Iggy to confirm SDK/server coherence"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "org.apache.iggy.bench.SmokeTest"
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
