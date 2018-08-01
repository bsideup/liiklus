package com.github.bsideup.liiklus.dynamodb;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorageTests;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.UUID;

import static java.util.Arrays.asList;

class DynamoDBPositionsStorageTest implements PositionsStorageTests {

    private static final LocalStackContainer localstack = new LocalStackContainer("0.8.6")
            .withServices(LocalStackContainer.Service.DYNAMODB)
            .withEnv(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, "accesskey")
            .withEnv(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, "secretkey");

    static {
        localstack.start();
    }

    private final AmazonDynamoDBAsync dynamoDB = AmazonDynamoDBAsyncClient.asyncBuilder()
            .withEndpointConfiguration(localstack.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
            .build();

    private final String tableName = UUID.randomUUID().toString();

    @Getter
    PositionsStorage storage = new DynamoDBPositionsStorage(dynamoDB, tableName);

    @Getter
    String topic = UUID.randomUUID().toString();

    @BeforeEach
    void setUp() {
        dynamoDB.createTable(new CreateTableRequest(
                asList(
                        new AttributeDefinition("topic", ScalarAttributeType.S),
                        new AttributeDefinition("groupId", ScalarAttributeType.S)
                ),
                tableName,
                asList(
                        new KeySchemaElement("topic", KeyType.HASH),
                        new KeySchemaElement("groupId", KeyType.RANGE)
                ),
                new ProvisionedThroughput(10L, 10L)
        ));
    }
}