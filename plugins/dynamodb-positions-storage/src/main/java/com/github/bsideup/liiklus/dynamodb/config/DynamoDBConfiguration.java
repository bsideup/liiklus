package com.github.bsideup.liiklus.dynamodb.config;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.github.bsideup.liiklus.dynamodb.DynamoDBPositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.google.auto.service.AutoService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.validation.annotation.Validated;
import reactor.core.scheduler.Schedulers;

import javax.validation.constraints.NotEmpty;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executors;

@Slf4j
@AutoService(ApplicationContextInitializer.class)
public class DynamoDBConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!"DYNAMODB".equals(environment.getProperty("storage.positions.type"))) {
            return;
        }

        var binder = Binder.get(environment);
        var dynamoDBProperties = binder.bind("dynamodb", DynamoDBProperties.class).orElseGet(DynamoDBProperties::new);


        applicationContext.registerBean(PositionsStorage.class, () -> {
            var builder = AmazonDynamoDBAsyncClient.asyncBuilder();

            dynamoDBProperties.getEndpoint()
                    .map(endpoint -> new AwsClientBuilder.EndpointConfiguration(
                            endpoint,
                            new DefaultAwsRegionProviderChain().getRegion()
                    ))
                    .ifPresent(builder::setEndpointConfiguration);

            var dynamoDB = builder
                    .withExecutorFactory(() -> Executors.newFixedThreadPool(Schedulers.DEFAULT_POOL_SIZE, new ThreadFactoryBuilder().setNameFormat("aws-dynamodb-%d").build()))
                    .build();

            if (dynamoDBProperties.isAutoCreateTable()) {
                log.info("Going to automatically create a table with name '{}'", dynamoDBProperties.getPositionsTable());
                dynamoDB.createTable(new CreateTableRequest(
                        Arrays.asList(
                                new AttributeDefinition(DynamoDBPositionsStorage.HASH_KEY_FIELD, ScalarAttributeType.S),
                                new AttributeDefinition(DynamoDBPositionsStorage.RANGE_KEY_FIELD, ScalarAttributeType.S)
                        ),
                        dynamoDBProperties.getPositionsTable(),
                        Arrays.asList(
                                new KeySchemaElement(DynamoDBPositionsStorage.HASH_KEY_FIELD, KeyType.HASH),
                                new KeySchemaElement(DynamoDBPositionsStorage.RANGE_KEY_FIELD, KeyType.RANGE)
                        ),
                        new ProvisionedThroughput(10L, 10L)
                ));
            }

            return new DynamoDBPositionsStorage(
                    dynamoDB,
                    dynamoDBProperties.getPositionsTable()
            );
        });
    }

    @Data
    @Validated
    public static class DynamoDBProperties {

        Optional<String> endpoint = Optional.empty();

        boolean autoCreateTable = false;

        @NotEmpty
        String positionsTable;
    }

}
