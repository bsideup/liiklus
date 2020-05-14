package com.github.bsideup.liiklus.transport.grpc;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.protocol.LiiklusEvent;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.Test;
import org.springframework.util.ResourceUtils;

import javax.net.ssl.SSLException;
import java.io.FileNotFoundException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static com.github.bsideup.liiklus.transport.grpc.GRPCAuthTest.getGRPCPort;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GRPCTLSTest {

    private static final LiiklusEvent LIIKLUS_EVENT_EXAMPLE = LiiklusEvent.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setType("com.example.event")
            .setSource("/tests")
            .setDataContentType("application/json")
            .putExtensions("comexampleextension1", "foo")
            .putExtensions("comexampleextension2", "bar")
            .setTime(ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
            .buildPartial();

    @Test
    public void shouldConnectWithTLS() throws SSLException, FileNotFoundException {
        var event = PublishRequest.newBuilder()
                .setTopic("authorized")
                .setLiiklusEvent(LIIKLUS_EVENT_EXAMPLE)
                .build();

        try (var app = new ApplicationRunner("MEMORY", "MEMORY")
                .withProperty("grpc.enabled", true)
                .withProperty("grpc.port", 0)
                .withProperty("grpc.tls.key", "file:src/test/resources/keys/tls/server0.key")
//                .withProperty("grpc.tls.keyPassword", "testsecret")
                .withProperty("grpc.tls.keyCertChain", "file:src/test/resources/keys/tls/server0.pem")
                .run()
        ) {
            int port = getGRPCPort(app);

            var unauthClient = new GRPCLiiklusClient(
                    NettyChannelBuilder
                            .forAddress("localhost", port)
                            .directExecutor()
                            .sslContext(GrpcSslContexts.forClient()
                                    .trustManager(ResourceUtils.getFile("file:src/test/resources/keys/tls/ca.pem"))
                                    .build()
                            )
                            .build()
            );

            assertThatThrownBy(() -> unauthClient.publish(event).block())
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining("UNAVAILABLE: Network closed for unknown reason");

        }
    }
}
