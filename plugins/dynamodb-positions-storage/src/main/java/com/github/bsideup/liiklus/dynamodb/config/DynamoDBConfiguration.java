package com.github.bsideup.liiklus.dynamodb.config;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.github.bsideup.liiklus.config.ExporterProfile;
import com.github.bsideup.liiklus.config.GatewayProfile;
import com.github.bsideup.liiklus.config.LiiklusConfiguration;
import com.github.bsideup.liiklus.dynamodb.DynamoDBPositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.google.auto.service.AutoService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import java.util.Arrays;
import java.util.Optional;

@Slf4j
@AutoService(LiiklusConfiguration.class)
@Configuration
@ExporterProfile
@GatewayProfile
@ConditionalOnProperty(value = "storage.positions.type", havingValue = "DYNAMODB")
@EnableConfigurationProperties(DynamoDBConfiguration.DynamoDBProperties.class)
public class DynamoDBConfiguration implements LiiklusConfiguration {

    @Autowired
    DynamoDBProperties dynamoDBProperties;

    @Bean(destroyMethod = "shutdown")
    AmazonDynamoDBAsync dynamoDb(DynamoDBProperties properties) {
        AmazonDynamoDBAsyncClientBuilder builder = AmazonDynamoDBAsyncClient.asyncBuilder();

        properties.getEndpoint()
                .map(endpoint -> new AwsClientBuilder.EndpointConfiguration(
                        endpoint,
                        new DefaultAwsRegionProviderChain().getRegion()
                ))
                .ifPresent(builder::setEndpointConfiguration);

        AmazonDynamoDBAsync dynamoDB = builder.build();

        if (properties.isAutoCreateTable()) {
            log.info("Going to automatically create a table with name '{}'", properties.getPositionsTable());
            dynamoDB.createTable(new CreateTableRequest(
                    Arrays.asList(
                            new AttributeDefinition("topic", ScalarAttributeType.S),
                            new AttributeDefinition("groupId", ScalarAttributeType.S)
                    ),
                    properties.getPositionsTable(),
                    Arrays.asList(
                            new KeySchemaElement("topic", KeyType.HASH),
                            new KeySchemaElement("groupId", KeyType.RANGE)
                    ),
                    new ProvisionedThroughput(10L, 10L)
            ));
        }

        return dynamoDB;
    }

    @Bean
    PositionsStorage dynamoDBPositionsStorage(AmazonDynamoDBAsync dynamoDB) {
        return new DynamoDBPositionsStorage(
                dynamoDB,
                dynamoDBProperties.getPositionsTable()
        );
    }

    @Data
    @ConfigurationProperties("dynamodb")
    @Validated
    public static class DynamoDBProperties {
        Optional<String> endpoint = Optional.empty();

        boolean autoCreateTable = false;

        @NotEmpty
        String positionsTable;
    }

}
