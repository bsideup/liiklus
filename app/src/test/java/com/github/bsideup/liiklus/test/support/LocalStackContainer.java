package com.github.bsideup.liiklus.test.support;

import com.amazonaws.SDKGlobalConfiguration;
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
        result.put(SDKGlobalConfiguration.AWS_REGION_SYSTEM_PROPERTY, "eu-central-1");
        result.put(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, "accesskey");
        result.put(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, "secretkey");
        result.put("dynamodb.endpoint", "http://localhost:" + getMappedPort(4569));
        return result;
    }
}
