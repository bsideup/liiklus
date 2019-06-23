package com.github.bsideup.liiklus.benchmark;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.LiiklusClient;
import com.github.bsideup.liiklus.RSocketLiiklusClient;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.github.bsideup.liiklus.service.ReactorLiiklusServiceImpl;
import io.grpc.netty.NettyChannelBuilder;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import lombok.SneakyThrows;
import org.openjdk.jmh.annotations.*;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput})
@OperationsPerInvocation(TransportBenchmark.LIMIT)
@Fork(1)
@Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Threads(1)
@Timeout(time = 15, timeUnit = TimeUnit.SECONDS)
public class TransportBenchmark {

    public static final int LIMIT = 1_000_000;

    public enum Transport {
        DIRECT,
        RSOCKET,
        GRPC,
        ;
    }

    @Param
    Transport transport;

    final ConfigurableApplicationContext applicationContext = Application.start(new String[]{
            "--storage.positions.type=FAKE",
            "--storage.records.type=FAKE",
    });

    LiiklusClient liiklusClient;

    @TearDown(Level.Trial)
    @SneakyThrows
    public void doTearDown() {
        applicationContext.close();
    }

    @Setup(Level.Invocation)
    public void doSetupInvocation() {
        switch (transport) {
            case DIRECT:
                liiklusClient = new LiiklusClientAdapter(
                        applicationContext.getBean(ReactorLiiklusServiceImpl.class)
                );
                break;
            case RSOCKET:
                var rSocket = RSocketFactory.connect()
                        .transport(TcpClientTransport.create(8081))
                        .start()
                        .block();

                liiklusClient = new RSocketLiiklusClient(rSocket) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        rSocket.dispose();
                    }
                };
                break;
            case GRPC:
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
                break;
        }
    }

    @TearDown(Level.Invocation)
    @SneakyThrows
    public void doTearDownInvocation() {
        liiklusClient.close();
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
