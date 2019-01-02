package com.github.bsideup.liiklus.benchmark;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import io.grpc.netty.NettyChannelBuilder;
import lombok.SneakyThrows;
import org.openjdk.jmh.annotations.*;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput})
@OperationsPerInvocation(GRpcBenchmark.LIMIT)
@Fork(1)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Threads(1)
@Timeout(time = 15, timeUnit = TimeUnit.SECONDS)
public class GRpcBenchmark {

    public static final int LIMIT = 1_000_000;

    @Param({"true", "false"})
    boolean useCacheForAllThreads;

    @Param({"true", "false"})
    boolean unpooledAllocator;

    @Param({"true", "false"})
    boolean directExecutor;

    ConfigurableApplicationContext applicationContext;

    LiiklusClient liiklusClient;

    @Setup(Level.Trial)
    public void doSetupTrial() {
        Properties properties = System.getProperties();
        new Application();
        System.setProperties(properties);

        System.setProperty("io.netty.allocator.useCacheForAllThreads", useCacheForAllThreads + "");
        if (unpooledAllocator) {
            System.setProperty("io.netty.allocator.type", "unpooled");
        }

        System.setProperty("grpc.directExecutor", directExecutor + "");

        applicationContext = Application.start(new String[]{
                "--plugins.dir=./plugins",
                "--plugins.pathMatcher=*/build/libs/*.jar",
                "--storage.positions.type=MEMORY",
                "--storage.records.type=FAKE",
        });

        var channel = NettyChannelBuilder.forAddress("localhost", 6565)
                .directExecutor()
                .usePlaintext()
                .keepAliveWithoutCalls(true)
                .keepAliveTime(150, TimeUnit.SECONDS)
                .build();

        liiklusClient = new GRPCLiiklusClient(channel) {
            @Override
            @SneakyThrows
            public void close() throws IOException {
                super.close();
                channel.shutdownNow();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
        };
    }

    @TearDown(Level.Trial)
    @SneakyThrows
    public void doTearDownTrial() {
        liiklusClient.close();
        applicationContext.close();
    }

    @Benchmark
    public void measure() {
        liiklusClient
                .subscribe(
                        SubscribeRequest.newBuilder()
                                .setTopic("topic")
                                .setGroup(UUID.randomUUID().toString())
                                .setGroupVersion(1)
                                .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
                                .build()
                )
                .map(SubscribeReply::getAssignment)
                .flatMap(
                        assignment -> liiklusClient
                                .receive(ReceiveRequest.newBuilder().setAssignment(assignment).build())
                                .limitRate(200),
                        Integer.MAX_VALUE,
                        Integer.MAX_VALUE
                )
                .take(LIMIT)
                .ignoreElements()
                .block();
    }
}
