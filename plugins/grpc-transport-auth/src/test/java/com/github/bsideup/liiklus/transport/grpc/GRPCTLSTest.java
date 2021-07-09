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
import lombok.Value;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileWriter;
import java.math.BigInteger;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.github.bsideup.liiklus.transport.grpc.GRPCAuthTest.getGRPCPort;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

class GRPCTLSTest {

    @Test
    void shouldConnectWithTLS() {
        GeneratedCert rootCA = createCertificate("ca", null, true);
        GeneratedCert server = createCertificate("localhost", rootCA, false);

        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", server.getPrivateKeyFile().toURI().toString())
                            .withProperty("grpc.tls.keyCertChain", server.getCertificateFile().toURI().toString());
                },
                port -> {
                    var sslContext = GrpcSslContexts.forClient()
                            .trustManager(ResourceUtils.getFile(rootCA.getCertificateFile().toURI()))
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
    void shouldFailOnPlaintext() {
        GeneratedCert rootCA = createCertificate("ca", null, true);
        GeneratedCert server = createCertificate("localhost", rootCA, false);
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", server.getPrivateKeyFile().toURI().toString())
                            .withProperty("grpc.tls.keyCertChain", server.getCertificateFile().toURI().toString());
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
    void shouldFailOnWrongCA() {
        GeneratedCert rootCA = createCertificate("ca", null, true);
        GeneratedCert server = createCertificate("localhost", rootCA, false);
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", server.getPrivateKeyFile().toURI().toString())
                            .withProperty("grpc.tls.keyCertChain", server.getCertificateFile().toURI().toString());
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
    void mTLS() {
        GeneratedCert rootCA = createCertificate("ca", null, true);
        GeneratedCert server = createCertificate("localhost", rootCA, false);
        GeneratedCert client = createCertificate("localhost", rootCA, false);
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", server.getPrivateKeyFile().toURI().toString())
                            .withProperty("grpc.tls.keyCertChain", server.getCertificateFile().toURI().toString())
                            .withProperty("grpc.tls.trustCert", rootCA.getCertificateFile().toURI().toString());
                },
                port -> {
                    var sslContext = GrpcSslContexts.forClient()
                            .trustManager(rootCA.getCertificateFile())
                            .keyManager(client.getCertificateFile(), client.getPrivateKeyFile())
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
    void shouldFailOnMutualTLSWithMissingCertClient() {
        GeneratedCert rootCA = createCertificate("ca", null, true);
        GeneratedCert server = createCertificate("localhost", rootCA, false);
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", server.getPrivateKeyFile().toURI().toString())
                            .withProperty("grpc.tls.keyCertChain", server.getCertificateFile().toURI().toString())
                            .withProperty("grpc.tls.trustCert", rootCA.getCertificateFile().toURI().toString());
                },
                port -> {
                    var sslContext = GrpcSslContexts.forClient()
                            .trustManager(rootCA.getCertificateFile())
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
    void shouldFailOnMutualTLSWithWrongCertClient() {
        GeneratedCert rootCA = createCertificate("ca", null, true);
        GeneratedCert server = createCertificate("localhost", rootCA, false);
        GeneratedCert client = createCertificate("localhost", null, false);
        withApp(
                app -> {
                    app
                            .withProperty("grpc.tls.key", server.getPrivateKeyFile().toURI().toString())
                            .withProperty("grpc.tls.keyCertChain", server.getCertificateFile().toURI().toString())
                            .withProperty("grpc.tls.trustCert", rootCA.getCertificateFile().toURI().toString());
                },
                port -> {
                    var sslContext = GrpcSslContexts.forClient()
                            .trustManager(rootCA.getCertificateFile())
                            .keyManager(client.getCertificateFile(), client.getPrivateKeyFile())
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

    @Value
    static class GeneratedCert {
        PrivateKey privateKey;
        File privateKeyFile;

        X509Certificate certificate;
        File certificateFile;
    }

    @SneakyThrows
    private GeneratedCert createCertificate(String cnName, GeneratedCert issuer, boolean isCA) {
        var certKeyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        var name = new X500Name("CN=" + cnName);

        X500Name issuerName;
        PrivateKey issuerKey;
        if (issuer == null) {
            issuerName = name;
            issuerKey = certKeyPair.getPrivate();
        } else {
            issuerName = new X500Name(issuer.getCertificate().getSubjectDN().getName());
            issuerKey = issuer.getPrivateKey();
        }

        var builder = new JcaX509v3CertificateBuilder(
                issuerName,
                BigInteger.valueOf(System.currentTimeMillis()),
                new Date(),
                new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)),
                name,
                certKeyPair.getPublic()
        );

        if (isCA) {
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCA));
        }

        var keyFile = File.createTempFile("key", ".key");
        try (var writer = new JcaPEMWriter(new FileWriter(keyFile))) {
            writer.writeObject(new PemObject("RSA PRIVATE KEY", certKeyPair.getPrivate().getEncoded()));
        }
        var certificate = new JcaX509CertificateConverter().getCertificate(
                builder.build(
                        new JcaContentSignerBuilder("SHA256WithRSA").build(issuerKey)
                )
        );

        var certFile = File.createTempFile("cert", ".pem");
        try (var writer = new JcaPEMWriter(new FileWriter(certFile))) {
            writer.writeObject(certificate);
        }

        return new GeneratedCert(
                certKeyPair.getPrivate(),
                keyFile,
                certificate,
                certFile
        );
    }
}
