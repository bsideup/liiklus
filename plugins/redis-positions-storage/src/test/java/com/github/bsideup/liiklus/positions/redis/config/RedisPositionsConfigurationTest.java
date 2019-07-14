package com.github.bsideup.liiklus.positions.redis.config;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.redis.RedisPositionsStorage;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;

import static org.assertj.core.api.Assertions.assertThat;

class RedisPositionsConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner()
            .withInitializer((ApplicationContextInitializer) new RedisPositionsConfiguration());

    @Test
    void should_skip_when_position_storage_not_redis() {
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
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
            assertThat(context)
                    .getBean(PositionsStorage.class)
                    .isInstanceOf(RedisPositionsStorage.class);
        });
    }
}
