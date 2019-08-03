package com.github.bsideup.liiklus.positions.redis.config;

import com.github.bsideup.liiklus.positions.redis.RedisPositionsStorage;
import com.github.bsideup.liiklus.util.PropertiesUtil;
import com.google.auto.service.AutoService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.validation.annotation.Validated;
import reactor.core.publisher.Mono;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;

@Slf4j
@AutoService(ApplicationContextInitializer.class)
public class RedisPositionsConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        var type = environment.getRequiredProperty("storage.positions.type");
        if(!"REDIS".equals(type)) {
            return;
        }

        var redisProperties = PropertiesUtil.bind(environment, new RedisProperties());

        applicationContext.registerBean(RedisPositionsStorage.class, () -> {
            var redisURI = RedisURI.builder()
                    .withHost(redisProperties.getHost())
                    .withPort(redisProperties.getPort())
                    .build();

            return new RedisPositionsStorage(
                    Mono
                            .fromCompletionStage(() -> RedisClient.create().connectAsync(StringCodec.UTF8, redisURI))
                            .cache(),
                    redisProperties.getPositionsProperties().getPrefix()
            );
        });
    }

    @ConfigurationProperties("redis")
    @Data
    @Validated
    public static class RedisProperties {

        @NotEmpty
        String host;

        @Min(1)
        int port = -1;

        PositionsProperties positionsProperties = new PositionsProperties();

        @Data
        @Validated
        static class PositionsProperties {

            @NotEmpty
            String prefix = "liiklus:positions:";
        }
    }
}
