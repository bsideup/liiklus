package com.github.bsideup.liiklus;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.RSAKeyProvider;
import com.avast.grpc.jwt.client.JwtCallCredentials;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import lombok.SneakyThrows;
import org.junit.Test;
import org.pf4j.PluginManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import reactor.core.publisher.Hooks;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GRPCAuthTest {

    static {
        System.setProperty("plugins.dir", "../plugins");
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

        Hooks.onOperatorDebug();
    }

    static ConfigurableApplicationContext startLiiklus(List<String> args) {
        return Application.start(args.stream().map(it -> "--" + it).toArray(String[]::new));
    }

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
    public void shouldPublishOnlyWithAuthHmac512() {
        var args = List.of(
                "storage.positions.type=MEMORY",
                "storage.records.type=MEMORY",
                "rsocket.enabled=false",
                "grpc.port=0",
                "grpc.auth.alg=HMAC512",
                "grpc.auth.secret=secret",
                "server.port=0"
        );
        var event = PublishRequest.newBuilder()
                .setTopic("authorized")
                .setValue(ByteString.copyFromUtf8("bar"))
                .build();

        try (var app = startLiiklus(args)) {
            int port = getGRPCPort(app);

            var unauthClient = new GRPCLiiklusClient(
                    ManagedChannelBuilder
                            .forAddress("localhost", port)
                            .directExecutor()
                            .usePlaintext()
                            .build()
            );

            assertThatThrownBy(() -> unauthClient.publish(event).block())
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining("UNAUTHENTICATED: authorization header not found");


            var wrongAuthClient = new GRPCLiiklusClient(
                    ManagedChannelBuilder
                            .forAddress("localhost", port)
                            .directExecutor()
                            .usePlaintext()
                            .intercept(authInterceptor(Algorithm.HMAC256("wrong")))
                            .build()
            );

            assertThatThrownBy(() -> wrongAuthClient.publish(event).block())
                    .isInstanceOf(StatusRuntimeException.class)
                    .hasMessageContaining("UNAUTHENTICATED: authorization header validation failed: The provided Algorithm doesn't match the one defined in the JWT's Header");


            var authenticatedClient = new GRPCLiiklusClient(
                    ManagedChannelBuilder
                            .forAddress("localhost", port)
                            .directExecutor()
                            .usePlaintext()
                            .intercept(authInterceptor(Algorithm.HMAC512("secret")))
                            .build()
            );

            authenticatedClient.publish(event).block();
        }
    }

    @Test
    public void shouldPublishWithAuthRsa512() {
        var args = List.of(
                "storage.positions.type=MEMORY",
                "storage.records.type=MEMORY",
                "rsocket.enabled=false",
                "grpc.port=0",
                "grpc.auth.alg=RSA512",
                "grpc.auth.keys.main=MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6b/6nQLIQQ8fHT4PcSyb" +
                        "hOLUE/237dgicbjsE7/Z/uPffuc36NTMJ122ppz6dWYnCrQ6CeTgAde4hlLE7Kvv" +
                        "aFiUbe5XKwSL8KV292XqrwRZhMI58TTTygcrBodYGzHy0Yytv703rz+9Qt5HO5BF" +
                        "02/+sM+Z0wlH6aXl3K3/2HfSOfitqnArBGaAs+PRNX2jlVKD1c9Cb7vo5L0X7q+6" +
                        "55uBErEoN7IHbj1u33qI/xEvPSycIiT2RXMGZkvDZH6mTsALel4aP4Qpp1NcE+kD" +
                        "itoBYAPTGgR4gBQveXZmD10yUVgJl2icINY3FvT9oJB6wgCY9+iTvufPppT1RPFH" +
                        "dQIDAQAB",
                "server.port=0"
        );
        var event = PublishRequest.newBuilder()
                .setTopic("authorized")
                .setValue(ByteString.copyFromUtf8("bar"))
                .build();

        try (var app = startLiiklus(args)) {
            int port = getGRPCPort(app);

            var authenticatedClient = new GRPCLiiklusClient(
                    ManagedChannelBuilder
                            .forAddress("localhost", port)
                            .directExecutor()
                            .usePlaintext()
                            .intercept(authInterceptor(Algorithm.RSA512(new RSAKeyProvider() {
                                @Override
                                public RSAPublicKey getPublicKeyById(String keyId) {
                                    return null; // not verifying
                                }

                                @SneakyThrows
                                @Override
                                public RSAPrivateKey getPrivateKey() {
                                    // you can convert openssh key format to a rsa with `ssh-keygen -p -m PEM -f private_openssh_key`
                                    // and then from the rsa to a pkcs8 by `openssl pkcs8 -topk8 -nocrypt -in private_openssh_key`
                                    String privateKeyContent = new String(Files.readAllBytes(Paths.get(
                                            ClassLoader.getSystemResource("keys/private_key_main_2048_pkcs8.pem").toURI()
                                    )))
                                            .replaceAll("\\n", "")
                                            .replace("-----BEGIN PRIVATE KEY-----", "")
                                            .replace("-----END PRIVATE KEY-----", "");

                                    var keySpecPKCS8 = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(privateKeyContent));
                                    return (RSAPrivateKey) KeyFactory.getInstance("RSA").generatePrivate(keySpecPKCS8);
                                }

                                @Override
                                public String getPrivateKeyId() {
                                    return "main"; // matches with grpc.auth.keys -> ["main"]
                                }
                            })))
                            .build()
            );

            authenticatedClient.publish(event).block();
        }
    }

    private ClientInterceptor authInterceptor(Algorithm alg) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> call, CallOptions headers, Channel next) {
                return next.newCall(call, headers.withCallCredentials(JwtCallCredentials.blocking(() -> JWT
                        .create()
                        .sign(alg)
                )));
            }
        };
    }
}
