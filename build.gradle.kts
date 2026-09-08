plugins {
    java
    application
}

repositories {
    mavenCentral()
    // Iggy snapshots (only needed if you later track master instead of 0.6.0):
    // maven { url = uri("https://repository.apache.org/content/repositories/snapshots/") }
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    // Apache Iggy Java SDK — PINNED pre-VSR release.
    // Coordinates are org.apache.iggy:iggy (NOT the archived rs.iggy:iggy-java-sdk).
    implementation("org.apache.iggy:iggy:0.6.0")

    // Kafka protocol client — drives the Redpanda side.
    implementation("org.apache.kafka:kafka-clients:3.8.0")

    // Latency histograms (used by the second profile; harmless here).
    implementation("org.hdrhistogram:HdrHistogram:2.2.2")

    // Minimal logging so SDK/Kafka noise doesn't pollute result output.
    implementation("org.slf4j:slf4j-simple:2.0.13")
}

application {
    mainClass = "org.apache.iggy.bench.BenchMain"
}

// ./gradlew bench --args="--target iggy --messages 1000000 --size 256 --batch 1000"
tasks.register<JavaExec>("bench") {
    group = "benchmark"
    description = "Run the throughput benchmark against --target iggy|redpanda"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "org.apache.iggy.bench.BenchMain"
}

// ./gradlew smokeTest  — proves SDK 0.6.0 <-> pinned server are protocol-coherent
tasks.register<JavaExec>("smokeTest") {
    group = "verification"
    description = "10-message round-trip against Iggy to detect the VSR trap"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "org.apache.iggy.bench.SmokeTest"
}

tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
