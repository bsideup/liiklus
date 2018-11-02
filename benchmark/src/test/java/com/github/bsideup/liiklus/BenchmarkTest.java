package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.github.bsideup.liiklus.service.ReactorLiiklusServiceImpl;
import com.google.common.collect.ImmutableMap;
import io.grpc.netty.NettyChannelBuilder;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.context.ApplicationContext;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RunWith(Parameterized.class)
@RequiredArgsConstructor
public class BenchmarkTest extends AbstractBenchmark {

    static final String TOPIC = "topic";

    enum Scenario {
        // Fake scenario to initialize everything. We could use @BeforeClass but then IntelliJ/Gradle will not show the output
        INIT {
            @Override
            LiiklusClient getClient() throws Exception {
                // See https://about.twitter.com/en_us/values/elections-integrity.html#data
                var smallDataSetURL = "https://storage.googleapis.com/twitter-election-integrity/hashed/iranian/iranian_tweets_csv_hashed.zip";
                var bigDataSetURL = "https://storage.googleapis.com/twitter-election-integrity/hashed/ira/ira_tweets_csv_hashed.zip";

                var kafka = new KafkaContainer("5.0.0")
                        .withEnv("KAFKA_NUM_PARTITIONS", "32");
                kafka.start();
                var bootstrapServers = kafka.getBootstrapServers();

                var dataPath = downloadDataIfMissing(new URL(bigDataSetURL));

                Callable<InputStream> inputStreamSupplier = () -> {
                    var zipInputStream = new ZipArchiveInputStream(Files.newInputStream(dataPath));
                    zipInputStream.getNextEntry();
                    return zipInputStream;
                };
                totalMsgs = populateTopic(TOPIC, bootstrapServers, inputStreamSupplier, 1, 5_000_000);

                applicationContext = Application.start(new String[]{
                        "--plugins.dir=../plugins",
                        "--plugins.pathMatcher=*/build/libs/*.jar",
                        "--kafka.bootstrapServers=" + bootstrapServers,
                        "--storage.positions.type=MEMORY",
                        "--storage.records.type=KAFKA",
                });
                return null;
            }
        },
        DIRECT {
            @Override
            LiiklusClient getClient() {
                return new LiiklusClientAdapter(
                        applicationContext.getBean(ReactorLiiklusServiceImpl.class)
                );
            }
        },
        RSOCKET {
            @Override
            LiiklusClient getClient() {
                return new RSocketLiiklusClient(
                        RSocketFactory.connect()
                                .transport(TcpClientTransport.create(8081))
                                .start()
                                .block()
                );
            }
        },
        GRPC {
            @Override
            LiiklusClient getClient() {
                return new GRPCLiiklusClient(
                        NettyChannelBuilder.forAddress("localhost", 6565)
                                .directExecutor()
                                .usePlaintext()
                                .keepAliveWithoutCalls(true)
                                .keepAliveTime(150, TimeUnit.SECONDS)
                                .build()
                );
            }
        },
        ;

        abstract LiiklusClient getClient() throws Exception;
    }

    @Parameters(name = "{index}: {0}")
    public static Iterable<Scenario> data() {
        return Arrays.asList(Scenario.values());
    }

    static ApplicationContext applicationContext;

    static int totalMsgs;

    final Scenario scenario;

    @Test
    public void test() throws Exception {
        var liiklusClient = scenario.getClient();
        if (liiklusClient == null) {
            return;
        }

        var processedItems = metrics.meter("processed");

        liiklusClient
                .subscribe(
                        SubscribeRequest.newBuilder()
                                .setTopic(TOPIC)
                                .setGroup("benchmark-" + scenario.name())
                                .setGroupVersion(1)
                                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                                .build()
                )
                .map(SubscribeReply::getAssignment)
                .flatMap(
                        assignment -> liiklusClient
                                .receive(ReceiveRequest.newBuilder().setAssignment(assignment).build())
                                .limitRate(200, 200)
                                // .delayElements(Duration.ofMillis(2))
                                .doOnNext(record -> processedItems.mark())
                        , Integer.MAX_VALUE, Integer.MAX_VALUE
                )
                .take(totalMsgs)
                .blockLast();
    }

    @SneakyThrows
    private static Path downloadDataIfMissing(URL dataUrl) {
        var dataPath = Paths.get("data.txt");

        if (!Files.exists(dataPath)) {
            log.info("Data file is missing. Downloading...");
            var tmpFile = dataPath.resolveSibling(dataPath.getFileName().toString() + ".downloading");
            tmpFile.toFile().deleteOnExit();
            try (var stream = dataUrl.openStream()) {
                Files.copy(stream, tmpFile);
                Files.move(tmpFile, dataPath);
            } finally {
                Files.deleteIfExists(tmpFile);
            }
            log.info("Saved as {}", dataPath);
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