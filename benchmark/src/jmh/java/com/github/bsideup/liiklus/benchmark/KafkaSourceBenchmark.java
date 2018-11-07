package com.github.bsideup.liiklus.benchmark;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.common.collect.ImmutableMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.openjdk.jmh.annotations.*;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput})
@OperationsPerInvocation(KafkaSourceBenchmark.LIMIT)
@Fork(1)
@Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Threads(1)
public class KafkaSourceBenchmark {

    public static final int LIMIT = 1_000_000;

    private static final String TOPIC = "topic";
    private static final int NUM_PARTITIONS = 32;

    ConfigurableApplicationContext applicationContext;

    List<? extends RecordsStorage.PartitionSource> partitionSources;

    Disposable disposable;

    @Setup(Level.Trial)
    @SneakyThrows
    public void doSetupTrial() {
        // See https://about.twitter.com/en_us/values/elections-integrity.html#data
        var smallDataSetURL = "https://storage.googleapis.com/twitter-election-integrity/hashed/iranian/iranian_tweets_csv_hashed.zip";
        var bigDataSetURL = "https://storage.googleapis.com/twitter-election-integrity/hashed/ira/ira_tweets_csv_hashed.zip";

        var kafka = new KafkaContainer("5.0.0")
                .withEnv("KAFKA_NUM_PARTITIONS", NUM_PARTITIONS + "");
        kafka.start();
        var bootstrapServers = kafka.getBootstrapServers();

        var dataPath = downloadDataIfMissing(new URL(smallDataSetURL));

        Callable<InputStream> inputStreamSupplier = () -> {
            var zipInputStream = new ZipArchiveInputStream(Files.newInputStream(dataPath));
            zipInputStream.getNextEntry();
            return zipInputStream;
        };
        var totalMsgs = populateTopic(TOPIC, bootstrapServers, inputStreamSupplier, 1, LIMIT);

        if (totalMsgs < LIMIT) {
            throw new IllegalStateException("Dataset is smaller than ops limit: " + totalMsgs + "< " + LIMIT);
        }

        applicationContext = Application.start(new String[]{
                "--plugins.dir=./plugins",
                "--plugins.pathMatcher=*/build/libs/*.jar",
                "--kafka.bootstrapServers=" + bootstrapServers,
                "--storage.positions.type=MEMORY",
                "--storage.records.type=KAFKA",
        });
    }

    @Setup(Level.Invocation)
    public void doSetupInvocation() {
        var recordsStorage = applicationContext.getBean(RecordsStorage.class);

        var subscription = Flux
                .from(
                        recordsStorage
                                .subscribe(
                                        TOPIC,
                                        UUID.randomUUID().toString(),
                                        Optional.of("earliest")
                                )
                                .getPublisher(() -> CompletableFuture.completedFuture(Map.of()))
                )
                .replay(1);

        disposable = subscription.connect();

        partitionSources = subscription
                .flatMap(Flux::fromStream)
                .distinct(RecordsStorage.PartitionSource::getPartition)
                .take(NUM_PARTITIONS)
                .collectList()
                .block(Duration.ofSeconds(10));
    }

    @TearDown(Level.Invocation)
    public void doTearDownInvocation() {
        disposable.dispose();
    }

    @TearDown(Level.Trial)
    public void doTearDown() {
        applicationContext.close();
    }

    @Benchmark
    @SneakyThrows
    public void measure() {
        Flux.fromIterable(partitionSources)
                .flatMap(
                        partitionSource -> Flux.from(partitionSource.getPublisher())
                        , Integer.MAX_VALUE, Integer.MAX_VALUE
                )
                .take(LIMIT)
                .ignoreElements()
                .block();
    }

    @SneakyThrows
    private static Path downloadDataIfMissing(URL dataUrl) {
        var dataPath = Paths.get("data.txt").toAbsolutePath();

        if (!Files.exists(dataPath)) {
            log.info("Data file ({}) is missing. Downloading...");
            var tmpFile = dataPath.resolveSibling(dataPath.getFileName().toString() + ".downloading");
            tmpFile.toFile().deleteOnExit();
            try (var stream = dataUrl.openStream()) {
                Files.copy(stream, tmpFile);
                Files.move(tmpFile, dataPath);
            } finally {
                Files.deleteIfExists(tmpFile);
            }
            log.info("Saved as {}", dataPath);
        } else {
            log.info("Data is already downloaded to {}", dataPath);
        }
        return dataPath;
    }

    @SneakyThrows
    private static int populateTopic(String topic, String bootstrapServers, Callable<InputStream> dataStreamSupplier, int skip, int limit) {
        log.info("Populating topic {}", topic);
        var producer = new KafkaProducer<String, String>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE,
                        ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy",
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );

        var i = new AtomicInteger();

        try (var reader = new BufferedReader(new InputStreamReader(dataStreamSupplier.call()))) {
            Flux.fromStream(reader::lines)
                    .skip(skip)
                    .take(limit)
                    .parallel(32)
                    .doOnNext(line -> {
                        producer.send(new ProducerRecord<>(topic, line));

                        var count = i.incrementAndGet();
                        if (count % 100_000 == 0) {
                            log.info("Another 100k records populated, total so far: {}", count);
                        }
                    })
                    .sequential()
                    .ignoreElements()
                    .block();
        }

        producer.flush();
        log.info("Total records: {}", i.get());

        return i.get();
    }
}
