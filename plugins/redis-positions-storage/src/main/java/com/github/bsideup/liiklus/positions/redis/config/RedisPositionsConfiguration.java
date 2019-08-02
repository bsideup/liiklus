package com.github.bsideup.liiklus.positions.redis.config;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.redis.RedisPositionsStorage;
import com.google.auto.service.AutoService;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.validation.annotation.Validated;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;
import reactor.core.publisher.Mono;

import javax.validation.Validation;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import java.util.function.Supplier;

@Slf4j
@AutoService(ApplicationContextInitializer.class)
public class RedisPositionsConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        var type = environment.getProperty("storage.positions.type");
        if(!"REDIS".equals(type)) {
            return;
        }

        var binder = Binder.get(environment);
        var validationBindHandler = new ValidationBindHandler(
                new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator())
        );
        var bindable = Bindable.of(RedisProperties.class).withExistingValue(new RedisProperties());
        var redisProperties = binder.bind("redis", bindable, validationBindHandler)
                .orElseGet(RedisProperties::new);

        applicationContext.registerBean(PositionsStorage.class, () -> {
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
