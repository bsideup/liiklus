package com.github.bsideup.liiklus.kafka.config;

import com.github.bsideup.liiklus.config.GatewayProfile;
import com.github.bsideup.liiklus.config.LiiklusConfiguration;
import com.github.bsideup.liiklus.kafka.KafkaRecordsStorage;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.auto.service.AutoService;
import lombok.Data;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import javax.validation.constraints.NotEmpty;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@AutoService(LiiklusConfiguration.class)
@Configuration
@GatewayProfile
@EnableConfigurationProperties(KafkaRecordsStorageConfiguration.KafkaProperties.class)
@ConditionalOnProperty(value = "storage.records.type", havingValue = "KAFKA")
public class KafkaRecordsStorageConfiguration implements LiiklusConfiguration {

    @Autowired
    KafkaProperties kafkaProperties;

    @Bean
    RecordsStorage reactorKafkaSource() {
        String bootstrapServers = kafkaProperties.getBootstrapServers();

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "liiklus-" + UUID.randomUUID().toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);

        SenderOptions<ByteBuffer, ByteBuffer> senderOptions = SenderOptions.<ByteBuffer, ByteBuffer>create(props)
                .stopOnError(false);

        return new KafkaRecordsStorage(
                bootstrapServers,
                KafkaSender.create(senderOptions)
        );
    }

    @Data
    @ConfigurationProperties("kafka")
    @Validated
    public static class KafkaProperties {

        @NotEmpty
        String bootstrapServers;
    }
}
