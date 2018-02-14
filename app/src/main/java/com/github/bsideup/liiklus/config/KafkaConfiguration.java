package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.source.ReactorKafkaSource;
import lombok.Data;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
@EnableConfigurationProperties(KafkaConfiguration.KafkaProperties.class)
public class KafkaConfiguration {

    @Autowired
    KafkaProperties kafkaProperties;

    @Bean
    ReactorKafkaSource reactorKafkaSource() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "liiklus-" + UUID.randomUUID().toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteBufferSerializer.class);

        SenderOptions<ByteBuffer, ByteBuffer> senderOptions = SenderOptions.<ByteBuffer, ByteBuffer>create(props)
                .stopOnError(false);

        return new ReactorKafkaSource(
                kafkaProperties.getBootstrapServers(),
                KafkaSender.create(senderOptions)
        );
    }

    @Data
    @ConfigurationProperties("kafka")
    public static class KafkaProperties {

        @NotEmpty
        String bootstrapServers;
    }
}
