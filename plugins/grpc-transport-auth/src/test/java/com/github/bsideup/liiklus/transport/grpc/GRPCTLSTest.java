package com.github.bsideup.liiklus.transport.grpc;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.protocol.LiiklusEvent;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import lombok.SneakyThrows;
import org.junit.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.springframework.util.ResourceUtils;

import java.util.UUID;
import java.util.function.Consumer;

import static com.github.bsideup.liiklus.transport.grpc.GRPCAuthTest.getGRPCPort;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

public class GRPCTLSTest {

    @Test
    public void shouldConnectWithTLS() {
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", "file:src/test/resources/keys/tls/server0.key")
                            .withProperty("grpc.tls.keyCertChain", "file:src/test/resources/keys/tls/server0.pem");
                },
                port -> {
                    var sslContext = GrpcSslContexts.forClient()
                            .trustManager(ResourceUtils.getFile("file:src/test/resources/keys/tls/ca.pem"))
                            .build();

                    var channel = NettyChannelBuilder
                            .forAddress("localhost", port)
                            .sslContext(sslContext)
                            .build();

                    publishWith(channel);
                }
        );
    }

    @Test
    public void shouldFailOnPlaintext() {
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", "file:src/test/resources/keys/tls/server0.key")
                            .withProperty("grpc.tls.keyCertChain", "file:src/test/resources/keys/tls/server0.pem");
                },
                port -> {
                    var channel = NettyChannelBuilder
                            .forAddress("localhost", port)
                            .usePlaintext()
                            .build();

                    assertThatThrownBy(() -> publishWith(channel))
                            .asInstanceOf(type(StatusRuntimeException.class))
                            .satisfies(Throwable::printStackTrace)
                            .returns(Status.Code.UNAVAILABLE, it -> it.getStatus().getCode());
                }
        );
    }

    @Test
    public void shouldFailOnWrongCA() {
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", "file:src/test/resources/keys/tls/server0.key")
                            .withProperty("grpc.tls.keyCertChain", "file:src/test/resources/keys/tls/server0.pem");
                },
                port -> {
                    var sslContext = GrpcSslContexts.forClient()
                            .build();

                    var channel = NettyChannelBuilder
                            .forAddress("localhost", port)
                            .sslContext(sslContext)
                            .build();

                    assertThatThrownBy(() -> publishWith(channel))
                            .asInstanceOf(type(StatusRuntimeException.class))
                            .satisfies(Throwable::printStackTrace)
                            .returns(Status.Code.UNAVAILABLE, it -> it.getStatus().getCode());
                }
        );
    }

    @Test
    public void mTLS() {
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", "file:src/test/resources/keys/tls/server0.key")
                            .withProperty("grpc.tls.keyCertChain", "file:src/test/resources/keys/tls/server0.pem")
                            .withProperty("grpc.tls.trustCert", "file:src/test/resources/keys/tls/ca.pem");
                },
                port -> {
                    var sslContext = GrpcSslContexts.forClient()
                            .trustManager(ResourceUtils.getFile("file:src/test/resources/keys/tls/ca.pem"))
                            .keyManager(
                                    ResourceUtils.getFile("file:src/test/resources/keys/tls/client.pem"),
                                    ResourceUtils.getFile("file:src/test/resources/keys/tls/client.key")
                            )
                            .build();

                    var channel = NettyChannelBuilder
                            .forAddress("localhost", port)
                            .sslContext(sslContext)
                            .build();

                    publishWith(channel);
                }
        );
    }

    @Test
    public void shouldFailOnMutualTLSWithMissingCertClient() {
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", "file:src/test/resources/keys/tls/server0.key")
                            .withProperty("grpc.tls.keyCertChain", "file:src/test/resources/keys/tls/server0.pem")
                            .withProperty("grpc.tls.trustCert", "file:src/test/resources/keys/tls/ca.pem");
                },
                port -> {
                    var sslContext = GrpcSslContexts.forClient()
                            .trustManager(ResourceUtils.getFile("file:src/test/resources/keys/tls/ca.pem"))
                            .build();

                    var channel = NettyChannelBuilder
                            .forAddress("localhost", port)
                            .sslContext(sslContext)
                            .build();

                    assertThatThrownBy(() -> publishWith(channel))
                            .asInstanceOf(type(StatusRuntimeException.class))
                            .satisfies(Throwable::printStackTrace)
                            .returns(Status.Code.UNAVAILABLE, it -> it.getStatus().getCode());
                }
        );
    }

    @SneakyThrows
    private void withApp(Consumer<ApplicationRunner> applicationRunnerConsumer, ThrowingConsumer<Integer> portConsumer) {
        var applicationRunner = new ApplicationRunner("MEMORY", "MEMORY")
                .withProperty("grpc.enabled", true)
                .withProperty("grpc.port", 0);
        applicationRunnerConsumer.accept(applicationRunner);
        try (var app = applicationRunner.run()) {
            portConsumer.accept(getGRPCPort(app));
        }
    }

    private void publishWith(ManagedChannel channel) {
        var event = PublishRequest.newBuilder()
                .setTopic("authorized")
                .setLiiklusEvent(
                        LiiklusEvent.newBuilder()
                                .setId(UUID.randomUUID().toString())
                                .setType("com.example.event")
                                .setSource("/tests")
                                .build()
                )
                .build();

        var client = new GRPCLiiklusClient(channel);
        client.publish(event).block();
    }
}
