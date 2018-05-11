package com.github.bsideup.liiklus.test.support;

import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;

public class LocalStackContainer extends GenericContainer<LocalStackContainer> {

    public LocalStackContainer() {
        super("localstack/localstack:0.7.4");

        withEnv("SERVICES", "dynamodb");
        withEnv("DEFAULT_REGION", "eu-central-1");
        withExposedPorts(4569);
    }

    public Map<String, String> getProperties() {
        Map<String, String> result = new HashMap<>();
        result.put("aws.region", "eu-central-1");
        result.put("aws.accessKeyId", "accesskey");
        result.put("aws.secretKey", "secretkey");
        result.put("dynamodb.endpoint", "http://localhost:" + getMappedPort(4569));
        return result;
    }
}
