package com.github.bsideup.liiklus.dynamodb;

import com.github.bsideup.liiklus.ApplicationRunner;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorageTests;
import lombok.Getter;
import org.springframework.context.ApplicationContext;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;

import java.util.UUID;

class DynamoDBPositionsStorageTest implements PositionsStorageTests {

    private static final LocalStackContainer localstack = new LocalStackContainer("0.8.6")
            .withServices(Service.DYNAMODB);

    static final ApplicationContext applicationContext;

    static {
        localstack.start();
        var endpointConfiguration = localstack.getEndpointConfiguration(Service.DYNAMODB);
        System.setProperty("aws.region", endpointConfiguration.getSigningRegion());
        var credentials = localstack.getDefaultCredentialsProvider().getCredentials();
        System.setProperty("aws.accessKeyId", credentials.getAWSAccessKeyId());
        System.setProperty("aws.secretAccessKey", credentials.getAWSSecretKey());

        applicationContext = new ApplicationRunner("MEMORY", "DYNAMODB")
                .withProperty("dynamodb.autoCreateTable", "true")
                .withProperty("dynamodb.positionsTable", "positions-" + UUID.randomUUID())
                .withProperty("dynamodb.endpoint", endpointConfiguration.getServiceEndpoint())
                .run();
    }

    @Getter
    PositionsStorage storage = applicationContext.getBean(PositionsStorage.class);
}