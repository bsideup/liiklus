package com.github.bsideup.liiklus.container;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;


public class LiiklusContainer extends GenericContainer<LiiklusContainer> {

    public LiiklusContainer(String version) {
        super("bsideup/liiklus:" + version);

        withEnv("spring_profiles_active", "gateway");
        withEnv("storage_positions_type", "MEMORY");
        withExposedPorts(6565);
    }

    public LiiklusContainer withKafka(KafkaContainer kafkaContainer) {
        return withKafka(kafkaContainer.getNetwork(), kafkaContainer.getNetworkAliases().get(0) + ":9092");
    }

    public LiiklusContainer withKafka(Network network, String bootstrapServers) {
        withNetwork(network);
        withEnv("kafka_bootstrapServers", bootstrapServers);
        return self();
    }

    public String getTarget() {
        return getContainerIpAddress() + ":" + getMappedPort(6565);
    }
}