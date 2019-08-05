package com.github.bsideup.liiklus.positions.redis.config;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.redis.RedisPositionsStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class RedisPositionsConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new RedisPositionsConfiguration());

    @Test
    void should_skip_when_position_storage_not_redis() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "storage.positions.type: FOO"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
        });
    }

    @Test
    void should_validate_properties() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "storage.positions.type: REDIS"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "redis.host: host"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "redis.port: 8888"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasNotFailed();
        });
    }

    @Test
    void should_register_positions_storage_bean_when_type_is_redis() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "storage.positions.type: REDIS",
                "redis.host: host",
                "redis.port: 8888"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasSingleBean(RedisPositionsStorage.class);
        });
    }
}
