package com.github.bsideup.liiklus.dynamodb;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorageTests;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.net.URI;
import java.util.UUID;

class DynamoDBPositionsStorageTest implements PositionsStorageTests {

    private static final LocalStackContainer localstack = new LocalStackContainer("0.8.6")
            .withServices(LocalStackContainer.Service.DYNAMODB);

    static {
        localstack.start();
    }

    private final DynamoDbAsyncClient dynamoDB = DynamoDbAsyncClient.builder()
            .region(Region.of(localstack.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB).getSigningRegion()))
            .endpointOverride(URI.create(localstack.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB).getServiceEndpoint()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "secret")))
            .build();

    private final String tableName = UUID.randomUUID().toString();

    @Getter
    PositionsStorage storage = new DynamoDBPositionsStorage(dynamoDB, tableName);

    @BeforeEach
    void setUp() {
        var request = CreateTableRequest.builder()
                .keySchema(
                        KeySchemaElement.builder().attributeName(DynamoDBPositionsStorage.HASH_KEY_FIELD).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(DynamoDBPositionsStorage.RANGE_KEY_FIELD).keyType(KeyType.RANGE).build()
                )
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(DynamoDBPositionsStorage.HASH_KEY_FIELD).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(DynamoDBPositionsStorage.RANGE_KEY_FIELD).attributeType(ScalarAttributeType.S).build()
                )
                .tableName(tableName)
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
                .build();

        try {
            dynamoDB.createTable(request).get();
        } catch (Exception e) {
            throw new IllegalStateException("Can't create positions dynamodb table", e);
        }
    }
}