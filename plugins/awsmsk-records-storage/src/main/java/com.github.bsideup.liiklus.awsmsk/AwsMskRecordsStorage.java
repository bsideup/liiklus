package com.github.bsideup.liiklus.awsmsk;

import com.github.bsideup.liiklus.awsmsk.auth.CertAndKeyParser;
import com.github.bsideup.liiklus.kafka.KafkaRecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.common.annotations.VisibleForTesting;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kafka.KafkaAsyncClient;
import software.amazon.awssdk.services.kafka.model.GetBootstrapBrokersRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

@Slf4j
@FieldDefaults(makeFinal = true)
public class AwsMskRecordsStorage implements RecordsStorage {

    static final String DEFAULT_TRUSTSTORE = Path.of(
            System.getProperty("java.home"), "lib", "security", "cacerts"
    ).toString();

    KafkaRecordsStorage kafkaRecordsStorage;

    @VisibleForTesting
    String bootstrapServer;

    @VisibleForTesting
    Map<String, String> baseProps;

    private final CertAndKeyParser certAndKeyParser = new CertAndKeyParser();

    public AwsMskRecordsStorage(KafkaAsyncClient kafkaClient, String arn, Optional<Authentication> authOpt) {
        final var props = new HashMap<String, String>();

        var brokerBootstrapInfo = getBroker(arn, kafkaClient);
        if (brokerBootstrapInfo.getType() == BrokerBootstrapInformation.Type.TLS) {
            // defaults to TLS if available
            props.put("security.protocol", "SSL");
            props.put("ssl.truststore.location", DEFAULT_TRUSTSTORE);
        }

        authOpt.map(this::setupStores).ifPresent(kafkaStore -> {
            props.put("ssl.keystore.location", kafkaStore.getKeystoreLocation());
            props.put("ssl.keystore.password", kafkaStore.getKeystorePassword());
            props.put("ssl.key.password", kafkaStore.getKeyPassword());
        });

        this.bootstrapServer = brokerBootstrapInfo.getBootstrapServer();
        this.baseProps = Map.copyOf(props);
        this.kafkaRecordsStorage = new KafkaRecordsStorage(this.bootstrapServer, this.baseProps);
    }

    @Override
    public CompletionStage<OffsetInfo> publish(Envelope envelope) {
        return kafkaRecordsStorage.publish(envelope);
    }

    @Override
    public Subscription subscribe(String topic, String groupName, Optional<String> autoOffsetReset) {
        return kafkaRecordsStorage.subscribe(topic, groupName, autoOffsetReset);
    }

    @SneakyThrows
    private BrokerBootstrapInformation getBroker(String arn, KafkaAsyncClient kafkaClient) {
        var request = GetBootstrapBrokersRequest.builder()
                .clusterArn(arn)
                .build();

        var response = kafkaClient.getBootstrapBrokers(request).get(); // does not make sense to wait unfortunately

        return Optional.ofNullable(response.bootstrapBrokerStringTls())
                .map(it -> new BrokerBootstrapInformation(BrokerBootstrapInformation.Type.TLS, it))
                .orElseGet(() -> new BrokerBootstrapInformation(BrokerBootstrapInformation.Type.PLAINTEXT, response.bootstrapBrokerString()));
    }

    @SneakyThrows
    private KafkaKeyStoreInformation setupStores(Authentication authentication) {
        var key = certAndKeyParser.toRSAPrivateKey(authentication.getPrivateKey());
        var chain = certAndKeyParser.toX509Certificates(authentication.getCertificateChain());

        var keystore = File.createTempFile("kafka-", UUID.randomUUID().toString() + ".jks");
        keystore.deleteOnExit();

        var keystorePassword = generatePassword();
        var keyPassword = generatePassword();
        var keyAlias = UUID.randomUUID().toString();

        var keyStore = KeyStore.getInstance("jks");
        keyStore.load(null);
        keyStore.setKeyEntry(keyAlias, key, keyPassword.toCharArray(), chain.toArray(new Certificate[0]));
        keyStore.store(new FileOutputStream(keystore), keystorePassword.toCharArray());

        return new KafkaKeyStoreInformation(
                keystore.getAbsolutePath(),
                keystorePassword,
                keyPassword
        );
    }

    @SneakyThrows
    private String generatePassword() {
        var password = new byte[512]; // 4096 bit
        SecureRandom.getInstanceStrong().nextBytes(password);
        return Base64.getEncoder().encodeToString(password);
    }

    @Value
    public static class Authentication {

        String certificateChain;

        String privateKey;

    }

    @Value
    private static class BrokerBootstrapInformation {

        Type type;

        String bootstrapServer;

        public enum Type {
            TLS, PLAINTEXT
        }

    }

    @Value
    private static class KafkaKeyStoreInformation {

        String keystoreLocation;

        String keystorePassword;

        String keyPassword;

    }

}
