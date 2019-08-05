package com.github.bsideup.liiklus.kafka.config;

import com.github.bsideup.liiklus.kafka.KafkaRecordsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaRecordsStorageConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new KafkaRecordsStorageConfiguration());

    @Test
    void shouldSkipWhenNotKafka() {
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
        });
    }

    @Test
    void shouldSkipIfNotGateway() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway",
                "storage.records.type: KAFKA"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
        });
    }

    @Test
    void shouldValidateProperties() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: gateway",
                "storage.records.type: KAFKA"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "kafka.bootstrapServers: host:9092"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasSingleBean(KafkaRecordsStorage.class);
        });
    }

    @Test
    void shouldRegisterWhenKafka() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: gateway",
                "storage.records.type: KAFKA",
                "kafka.bootstrapServers: host:9092"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasSingleBean(KafkaRecordsStorage.class);
        });
    }

}