package com.github.bsideup.liiklus.records.inmemory.config;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.records.inmemory.InMemoryRecordsStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryRecordsConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new InMemoryRecordsConfiguration());

    @Test
    void shouldSkipWhenNotInMemory() {
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
        });
    }

    @Test
    void shouldSkipIfNotGateway() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: not_gateway",
                "storage.records.type: MEMORY"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
        });
    }

    @Test
    void shouldRegisterWhenInMemory() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "spring.profiles.active: gateway",
                "storage.records.type: MEMORY"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasSingleBean(InMemoryRecordsStorage.class);
        });
    }

}