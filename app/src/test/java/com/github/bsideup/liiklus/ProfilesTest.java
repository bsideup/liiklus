package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.common.collect.Sets;
import lombok.val;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.After;
import org.junit.Test;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;

public class ProfilesTest extends AbstractIntegrationTest {

    static Set<String> KAFKA_PROPERTIES = AbstractIntegrationTest.getKafkaProperties();

    static Set<String> DYNAMODB_PROPERTIES = AbstractIntegrationTest.getDynamoDBProperties();

    Set<String> commonArgs = Sets.newHashSet("server.port=0", "grpc.inProcessServerName=liiklus-profile-test");

    ConfigurableApplicationContext lastApplicationContext;

    @After
    public void tearDown() throws Exception {
        if (lastApplicationContext != null) {
            lastApplicationContext.close();
        }
    }

    @Test
    public void testRequired() throws Exception {
        assertThatAppWithProps(commonArgs)
                .hasRootCauseInstanceOf(BindValidationException.class);

        assertThatAppWithProps(commonArgs, KAFKA_PROPERTIES)
                .hasRootCauseInstanceOf(BindValidationException.class);

        assertThatAppWithProps(commonArgs, DYNAMODB_PROPERTIES)
                .hasRootCauseInstanceOf(BindValidationException.class);

        assertThatAppWithProps(commonArgs, KAFKA_PROPERTIES, DYNAMODB_PROPERTIES)
                .doesNotThrowAnyException();
    }

    @Test
    public void testExporterProfile() throws Exception {
        commonArgs.add("spring.profiles.active=exporter");

        assertThatAppWithProps(commonArgs)
                .hasRootCauseInstanceOf(BindValidationException.class);

        assertThatAppWithProps(commonArgs, DYNAMODB_PROPERTIES)
                .doesNotThrowAnyException();
    }

    @Test
    public void testGatewayProfile() throws Exception {
        commonArgs.add("spring.profiles.active=gateway");

        assertThatAppWithProps(commonArgs)
                .hasRootCauseInstanceOf(BindValidationException.class);

        assertThatAppWithProps(commonArgs, KAFKA_PROPERTIES)
                .hasRootCauseInstanceOf(BindValidationException.class);

        assertThatAppWithProps(commonArgs, DYNAMODB_PROPERTIES)
                .hasRootCauseInstanceOf(BindValidationException.class);

        assertThatAppWithProps(commonArgs, KAFKA_PROPERTIES, DYNAMODB_PROPERTIES)
                .doesNotThrowAnyException();
    }

    @SafeVarargs
    protected final AbstractThrowableAssert<?, ? extends Throwable> assertThatAppWithProps(Set<String>... props) {
        if (lastApplicationContext != null) {
            lastApplicationContext.close();
        }

        return assertThatCode(() -> {
            val args = Stream.of(props)
                    .flatMap(Collection::stream)
                    .map(it -> "--" + it)
                    .toArray(String[]::new);

            lastApplicationContext = Application.start(args);
        });
    }

}
