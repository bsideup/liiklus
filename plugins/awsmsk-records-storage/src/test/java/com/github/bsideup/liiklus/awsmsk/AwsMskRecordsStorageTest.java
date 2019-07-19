package com.github.bsideup.liiklus.awsmsk;

import lombok.SneakyThrows;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.kafka.KafkaAsyncClient;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersRequest;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class AwsMskRecordsStorageTest {

    @Mock
    KafkaAsyncClient kafkaClientMock;

    AwsMskRecordsStorage underTest;

    String testArn = UUID.randomUUID().toString();

    @Test
    void shouldFetchBootstrapServerWithARNAndPreferTLS() {
        mockBootstrapServerResponse(
                testArn,
                GetBootstrapBrokersResponse.builder()
                        .bootstrapBrokerString("127.0.0.1:9092")
                        .bootstrapBrokerStringTls("127.0.0.1:9094")
                        .build()
        );

        underTest = new AwsMskRecordsStorage(kafkaClientMock, testArn, Optional.empty());
        assertThat(underTest.bootstrapServer)
                .isEqualTo("127.0.0.1:9094");
    }

    @Test
    void shouldFetchBootstrapServerWithARNAndFallbackToPlainIfNoTLS() {
        mockBootstrapServerResponse(
                testArn,
                GetBootstrapBrokersResponse.builder()
                        .bootstrapBrokerString("127.0.0.1:9092")
                        .build()
        );

        underTest = new AwsMskRecordsStorage(kafkaClientMock, testArn, Optional.empty());
        assertThat(underTest.bootstrapServer)
                .isEqualTo("127.0.0.1:9092");
    }

    @Test
    void shouldSetUpTLSConfigurationIfConnectingOverTLS() {
        mockBootstrapServerResponse(
                testArn,
                GetBootstrapBrokersResponse.builder()
                        .bootstrapBrokerStringTls("127.0.0.1:9094")
                        .build()
        );

        underTest = new AwsMskRecordsStorage(kafkaClientMock, testArn, Optional.empty());
        assertThat(underTest.baseProps)
                .containsEntry("security.protocol", "SSL")
                .containsEntry("ssl.truststore.location", AwsMskRecordsStorage.DEFAULT_TRUSTSTORE);
    }

    @Test
    void shouldNotSetUpTLSConfigurationIfNotConnectingOverTLS() {
        mockBootstrapServerResponse(
                testArn,
                GetBootstrapBrokersResponse.builder()
                        .bootstrapBrokerString("127.0.0.1:9092")
                        .build()
        );

        underTest = new AwsMskRecordsStorage(kafkaClientMock, testArn, Optional.empty());
        assertThat(underTest.baseProps)
                .doesNotContainKey("security.protocol")
                .doesNotContainKey("ssl.truststore.location");
    }

    @Test
    void shouldSetUpAuthenticationIfPassedAuthenticationObject() {
        mockBootstrapServerResponse(
                testArn,
                GetBootstrapBrokersResponse.builder()
                        .bootstrapBrokerStringTls("127.0.0.1:9094")
                        .build()
        );

        underTest = new AwsMskRecordsStorage(kafkaClientMock, testArn, Optional.of(generateTestAuth()));

        assertThat(underTest.baseProps)
                .containsKey("ssl.keystore.location")
                .containsKey("ssl.keystore.password")
                .containsKey("ssl.key.password");
    }

    @SneakyThrows
    private AwsMskRecordsStorage.Authentication generateTestAuth() {
        var keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        var keyPair = keyGen.generateKeyPair();

        var name = new X500Name("CN=" + UUID.randomUUID().toString());
        var certificateBuilder = new X509v3CertificateBuilder(
                name,
                new BigInteger(32, new SecureRandom()),
                Date.from(Instant.now()),
                Date.from(Instant.now().plus(5, ChronoUnit.DAYS)),
                name,
                SubjectPublicKeyInfo.getInstance(ASN1Sequence.getInstance(keyPair.getPublic().getEncoded()))
        );

        var signatureAlgorithm = new DefaultSignatureAlgorithmIdentifierFinder().find("sha512WithRSAEncryption");
        var digestAlgorithm = new DefaultDigestAlgorithmIdentifierFinder().find(signatureAlgorithm);
        var asymmetricKeyParameter = PrivateKeyFactory.createKey(keyPair.getPrivate().getEncoded());
        var cs = new BcRSAContentSignerBuilder(signatureAlgorithm, digestAlgorithm).build(asymmetricKeyParameter);
        var certHolder = certificateBuilder.build(cs);
        var cert = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(
                new ByteArrayInputStream(certHolder.toASN1Structure().getEncoded())
        );

        return new AwsMskRecordsStorage.Authentication(
                toPemFormat(cert),
                toPem(keyPair.getPrivate())
        );
    }

    private void mockBootstrapServerResponse(String arn, GetBootstrapBrokersResponse result) {
        Mockito
                .when(kafkaClientMock.getBootstrapBrokers(GetBootstrapBrokersRequest.builder()
                        .clusterArn(arn)
                        .build()))
                .thenReturn(Mono.just(result).toFuture());
    }

    private String toPemFormat(X509Certificate certificate) {
        var stringWriter = new StringWriter();
        try (var pemWriter = new PemWriter(stringWriter)) {
            var pemGenerator = new JcaMiscPEMGenerator(certificate);
            pemWriter.writeObject(pemGenerator);
            pemWriter.flush();
            stringWriter.flush();
            return stringWriter.toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to print certificate", e);
        }
    }

    @SneakyThrows
    private String toPem(PrivateKey privateKey) {
        var publicKey = new PemObject("PRIVATE KEY", privateKey.getEncoded());

        var outputStream = new ByteArrayOutputStream();
        try (PemWriter pemWriter = new PemWriter(new OutputStreamWriter(outputStream))) {
            pemWriter.writeObject(publicKey);
        }

        return new String(outputStream.toByteArray());
    }

}