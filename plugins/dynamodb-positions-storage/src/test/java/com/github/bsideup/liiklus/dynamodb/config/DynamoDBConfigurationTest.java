package com.github.bsideup.liiklus.dynamodb.config;

import com.github.bsideup.liiklus.dynamodb.DynamoDBPositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.BeansException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class DynamoDBConfigurationTest {

    ApplicationContextRunner applicationContextRunner = new ApplicationContextRunner(() -> new StaticApplicationContext() {
        @Override
        public void refresh() throws BeansException, IllegalStateException {
        }
    })
            .withInitializer((ApplicationContextInitializer) new DynamoDBConfiguration());

    @Test
    void shouldSkipWhenNotDynamoDB() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "storage.positions.type: FOO"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).doesNotHaveBean(PositionsStorage.class);
        });
    }

    @Test
    void shouldValidateProperties() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "storage.positions.type: DYNAMODB"
        );
        applicationContextRunner.run(context -> {
            assertThat(context)
                    .getFailure()
                    .hasCauseInstanceOf(BindValidationException.class);
        });
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "dynamodb.positionsTable: foo"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasNotFailed();
        });
    }

    @Test
    void shouldRegisterWhenDynamoDB() {
        applicationContextRunner = applicationContextRunner.withPropertyValues(
                "storage.positions.type: DYNAMODB",
                "dynamodb.positionsTable: foo"
        );
        applicationContextRunner.run(context -> {
            assertThat(context).hasSingleBean(DynamoDBPositionsStorage.class);
        });
    }

}