package com.github.bsideup.liiklus.dynamodb.config;

import com.github.bsideup.liiklus.dynamodb.DynamoDBPositionsStorage;
import com.github.bsideup.liiklus.util.PropertiesUtil;
import com.google.auto.service.AutoService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.validation.annotation.Validated;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import javax.validation.constraints.NotEmpty;
import java.net.URI;
import java.util.Optional;

@Slf4j
@AutoService(ApplicationContextInitializer.class)
public class DynamoDBConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!"DYNAMODB".equals(environment.getRequiredProperty("storage.positions.type"))) {
            return;
        }

        var dynamoDBProperties = PropertiesUtil.bind(environment, new DynamoDBProperties());

        applicationContext.registerBean(DynamoDBPositionsStorage.class, () -> {
            var builder = DynamoDbAsyncClient.builder();

            dynamoDBProperties.getEndpoint()
                    .map(URI::create)
                    .ifPresent(builder::endpointOverride);

            var dynamoDB = builder
                    .build();

            if (dynamoDBProperties.isAutoCreateTable()) {
                log.info("Going to automatically create a table with name '{}'", dynamoDBProperties.getPositionsTable());
                var request = CreateTableRequest.builder()
                        .keySchema(
                                KeySchemaElement.builder().attributeName(DynamoDBPositionsStorage.HASH_KEY_FIELD).keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName(DynamoDBPositionsStorage.RANGE_KEY_FIELD).keyType(KeyType.RANGE).build()
                        )
                        .attributeDefinitions(
                                AttributeDefinition.builder().attributeName(DynamoDBPositionsStorage.HASH_KEY_FIELD).attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName(DynamoDBPositionsStorage.RANGE_KEY_FIELD).attributeType(ScalarAttributeType.S).build()
                                )
                        .tableName(dynamoDBProperties.getPositionsTable())
                        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build())
                        .build();

                try {
                    dynamoDB.createTable(request).get();
                } catch (Exception e) {
                    throw new IllegalStateException("Can't create positions dynamodb table", e);
                }
            }

            return new DynamoDBPositionsStorage(
                    dynamoDB,
                    dynamoDBProperties.getPositionsTable()
            );
        });
    }

    @ConfigurationProperties("dynamodb")
    @Data
    @Validated
    public static class DynamoDBProperties {

        Optional<String> endpoint = Optional.empty();

        boolean autoCreateTable = false;

        @NotEmpty
        String positionsTable;
    }

}
