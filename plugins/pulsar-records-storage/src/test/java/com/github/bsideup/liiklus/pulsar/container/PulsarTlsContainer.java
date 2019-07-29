package com.github.bsideup.liiklus.pulsar.container;

import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class PulsarTlsContainer extends PulsarContainer {

    public PulsarTlsContainer(String pulsarVersion) {
        super(pulsarVersion);
        withExposedPorts(BROKER_HTTP_PORT, BROKER_PORT, 6651, 8443);
        withEnv("PULSAR_PREFIX_brokerServicePortTls", "6651");
        withEnv("PULSAR_PREFIX_webServicePortTls", "8443");
        withEnv("PULSAR_PREFIX_tlsEnabled", "true");
        withEnv("PULSAR_PREFIX_tlsCertificateFilePath", "/pulsar/broker.cert.pem");
        withEnv("PULSAR_PREFIX_tlsKeyFilePath", "/pulsar/broker.key-pk8.pem");
        withEnv("PULSAR_PREFIX_tlsTrustCertsFilePath", "/pulsar/ca.cert.pem");

        Path resourceDirectory = getTestCertsFolder();
        Arrays.stream(resourceDirectory.toFile().listFiles())
                .forEach(certFile -> {
                    var certFilePath = certFile.toPath().toAbsolutePath();
                    var fileName = certFilePath.getFileName().toString();
                    withCopyFileToContainer(
                            MountableFile.forHostPath(certFilePath),
                            "/pulsar/" + fileName
                    );
                });

        setCommand(
                "/bin/bash",
                "-c",
                "bin/apply-config-from-env.py conf/standalone.conf && bin/pulsar standalone --no-functions-worker -nss"
        );
    }

    private Path getTestCertsFolder() {
        return Paths.get("src", "test", "resources", "certs");
    }

    public Path getCaCert() {
        return getTestCertsFolder().resolve("ca.cert.pem");
    }

    public Path getUserCert() {
        return getTestCertsFolder().resolve("user1.cert.pem");
    }

    public Path getUserKey() {
        return getTestCertsFolder().resolve("user1.key-pk8.pem");
    }

    public PulsarTlsContainer withTlsAuthentication() {
        withEnv("PULSAR_PREFIX_authenticationEnabled", "true");
        withEnv("PULSAR_PREFIX_authenticationProviders", "org.apache.pulsar.broker.authentication.AuthenticationProviderTls");
        return this;
    }

    public String getPulsarTlsBrokerUrl() {
        return String.format("pulsar+ssl://%s:%s", getContainerIpAddress(), getMappedPort(6651));
    }

    public String getHttpsServiceUrl() {
        return String.format("https://%s:%s", getContainerIpAddress(), getMappedPort(8443));
    }
}
