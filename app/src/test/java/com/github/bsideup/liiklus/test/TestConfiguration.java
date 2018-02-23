package com.github.bsideup.liiklus.test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.*;
import com.github.bsideup.liiklus.dynamodb.DynamoDBPositionsStorage;
import com.github.bsideup.liiklus.dynamodb.config.DynamoDBConfiguration.DynamoDBProperties;
import lombok.SneakyThrows;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@org.springframework.boot.test.context.TestConfiguration
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TestConfiguration {

    @SneakyThrows
    public TestConfiguration(DynamoDBProperties dynamoDBProperties, AmazonDynamoDBAsync dynamoDB) {
        Future<CreateTableResult> positionsTableFuture = dynamoDB.createTableAsync(new CreateTableRequest(
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

        positionsTableFuture.get(1, TimeUnit.MINUTES);
    }
}
