package com.github.bsideup.liiklus.transport.grpc;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.GRPCLiiklusClient;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import lombok.SneakyThrows;
import org.junit.Test;
import org.pf4j.PluginManager;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ResourceUtils;

import javax.net.ssl.SSLException;
import java.io.FileNotFoundException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GRPCTLSTest {

    @SneakyThrows
    static int getGRPCPort(ApplicationContext ctx) {
        var pluginManager = ctx.getBean(PluginManager.class);

        var classLoader = pluginManager.getPluginClassLoader("grpc-transport");
        var serverClazz = classLoader.loadClass(Server.class.getName());
        var getPortMethod = serverClazz.getDeclaredMethod("getPort");
        var server = ctx.getBean(serverClazz);

        return (int) getPortMethod.invoke(server);
    }


    @Test
    public void shouldPublishOnlyWithAuthHmac512() throws SSLException, FileNotFoundException {
        var event = PublishRequest.newBuilder()
                .setTopic("authorized")
                .setValue(ByteString.copyFromUtf8("bar"))
                .build();

        try (var app = new ApplicationRunner("MEMORY", "MEMORY")
                .withProperty("grpc.enabled", true)
                .withProperty("grpc.port", 0)
                .withProperty("grpc.tls.key", "file:/Users/lanwen/code/github.com/bsideup/liiklus/pki/private/server.pkcs8.key") //didn't get where relative path
//                .withProperty("grpc.tls.keyPassword", "testsecret")
                .withProperty("grpc.tls.keyCertChain", "file:/Users/lanwen/code/github.com/bsideup/liiklus/pki/issued/server.crt")
                .run()
        ) {
            int port = getGRPCPort(app);

            var unauthClient = new GRPCLiiklusClient(
                    NettyChannelBuilder
                            .forAddress("localhost", port)
                            .directExecutor()
                            .sslContext(GrpcSslContexts.forClient()
                                    .trustManager(ResourceUtils.getFile("/Users/lanwen/code/github.com/bsideup/liiklus/pki/issued/server.crt"))
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
