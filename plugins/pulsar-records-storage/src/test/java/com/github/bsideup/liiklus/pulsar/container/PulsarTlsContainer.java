package com.github.bsideup.liiklus.pulsar.container;

import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PulsarTlsContainer extends PulsarContainer {

    public static final int BROKER_TLS_PORT = 6651;

    public PulsarTlsContainer() {
        this("2.4.0");
    }

    public PulsarTlsContainer(String pulsarVersion) {
        super(pulsarVersion);
        withExposedPorts(BROKER_TLS_PORT, BROKER_HTTP_PORT);
        withEnv("PULSAR_PREFIX_brokerServicePortTls", BROKER_TLS_PORT + "");
        withEnv("PULSAR_PREFIX_tlsEnabled", "true");
        withEnv("PULSAR_PREFIX_tlsCertificateFilePath", "/pulsar/broker.cert.pem");
        withEnv("PULSAR_PREFIX_tlsKeyFilePath", "/pulsar/broker.key-pk8.pem");
        withEnv("PULSAR_PREFIX_tlsTrustCertsFilePath", "/pulsar/ca.cert.pem");

        withCopyFileToContainer(MountableFile.forClasspathResource("certs/"), "/pulsar/");

        setCommand(
                "/bin/bash",
                "-c",
                "bin/apply-config-from-env.py conf/standalone.conf && " +
                "bin/apply-config-from-env.py conf/proxy.conf && " +
                        "bin/pulsar standalone --no-functions-worker -nss"
        );

        waitingFor(Wait.forLogMessage(".*Created namespace public\\/default.*", 1));
    }

    public Path getCaCert() {
        return Paths.get("src", "test", "resources", "certs").resolve("ca.cert.pem");
    }

    @Override
    public String getPulsarBrokerUrl() {
        return String.format("pulsar+ssl://%s:%s", getContainerIpAddress(), getMappedPort(BROKER_TLS_PORT));
    }
}
