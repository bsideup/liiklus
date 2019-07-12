package com.github.bsideup.liiklus.positions.redis.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class RedisPositionsConfigurationTest {
    @InjectMocks RedisPositionsConfiguration redisPositionsConfiguration;

    MockEnvironment           configurableEnvironment;
    GenericApplicationContext genericApplicationContext;

    @BeforeEach
    void setUp() {
        configurableEnvironment = new MockEnvironment();
        genericApplicationContext = new GenericApplicationContext();
        genericApplicationContext.setEnvironment(configurableEnvironment);
    }

    @Test
    void should_skip_when_position_storage_not_redis() {
        //given

        //when
        redisPositionsConfiguration.initialize(this.genericApplicationContext);
        genericApplicationContext.refresh();

        //then
        assertThat(genericApplicationContext.getBeanDefinitionNames()).hasSize(0);

    }

    @Test
    void should_register_positions_storage_bean_when_type_is_redis() {
        //given
        configurableEnvironment.setProperty("storage.positions.type", "REDIS");
        configurableEnvironment.setProperty("redis.host", "host");
        configurableEnvironment.setProperty("redis.port", "8888");

        //when
        redisPositionsConfiguration.initialize(genericApplicationContext);

        //then
        assertThat(genericApplicationContext.getBeanDefinitionNames().length).isGreaterThan(1);
    }

}
