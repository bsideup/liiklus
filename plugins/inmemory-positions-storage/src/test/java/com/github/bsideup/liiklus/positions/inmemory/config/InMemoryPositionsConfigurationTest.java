package com.github.bsideup.liiklus.positions.inmemory.config;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.inmemory.InMemoryPositionsStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryPositionsConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new InMemoryPositionsConfiguration());

    @Test
    void shouldSkipWhenNotInMemory() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "storage.positions.type: FOO"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
        });
    }

    @Test
    void shouldRegisterWhenInMemory() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "storage.positions.type: MEMORY"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasSingleBean(InMemoryPositionsStorage.class);
        });
    }
}