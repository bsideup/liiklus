package com.github.bsideup.liiklus.positions.redis.config;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.redis.RedisPositionsStorage;
import com.google.auto.service.AutoService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;

@Slf4j
@AutoService(ApplicationContextInitializer.class)
public class RedisPositionsConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        ConfigurableEnvironment environment = applicationContext.getEnvironment();

        var type = environment.getProperty("storage.positions.type");
        if(!"REDIS".equals(type)) {
            return;
        }

        RedisProperties redisProperties = Binder.get(environment)
                .bind("redis", RedisProperties.class)
                .orElseGet(RedisProperties::new);

        applicationContext.registerBean(PositionsStorage.class,
                () -> {
                    RedisURI redisURI = RedisURI.builder()
                            .withHost(redisProperties.getHost())
                            .withPort(redisProperties.getPort())
                            .build();

                    RedisClient redisClient = RedisClient.create(redisURI);

                    return new RedisPositionsStorage(redisClient);
                },
                bd -> bd.setDestroyMethodName("shutdown")
        );

    }

    @Data
    @Validated
    public static class RedisProperties {
        @NotEmpty String host;
        @NotEmpty int    port;
    }
}
