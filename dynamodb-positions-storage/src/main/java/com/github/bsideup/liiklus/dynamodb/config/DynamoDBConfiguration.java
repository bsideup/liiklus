package com.github.bsideup.liiklus.dynamodb.config;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.github.bsideup.liiklus.dynamodb.DynamoDBPositionsStorage;
import lombok.Data;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

@Configuration
@ConditionalOnProperty(value = "storage.positions.type", havingValue = "DYNAMODB", matchIfMissing = true)
@EnableConfigurationProperties(DynamoDBConfiguration.DynamoDBProperties.class)
public class DynamoDBConfiguration {

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

        return builder.build();
    }

    @Bean
    DynamoDBPositionsStorage dynamoDBPositionsStorage(AmazonDynamoDBAsync dynamoDB) {
        return new DynamoDBPositionsStorage(
                dynamoDB,
                dynamoDBProperties.getPositionsTable()
        );
    }

    @Data
    @ConfigurationProperties("dynamodb")
    public static class DynamoDBProperties {
        Optional<String> endpoint = Optional.empty();

        @NotEmpty
        String positionsTable;
    }

}
