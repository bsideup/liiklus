package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.test.AbstractIntegrationTest;
import com.google.common.collect.Sets;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;

public class ProfilesTest extends AbstractIntegrationTest {

    static Set<String> RECORDS_PROPERTIES = Sets.newHashSet(
            "storage.records.type=MEMORY"
    );

    static Set<String> POSITIONS_PROPERTIES = Sets.newHashSet(
            "storage.positions.type=MEMORY"
    );

    Set<String> commonArgs = Sets.newHashSet();

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
                .hasMessageContaining("Required key");

        assertThatAppWithProps(commonArgs, RECORDS_PROPERTIES)
                .hasMessageContaining("Required key");

        assertThatAppWithProps(commonArgs, POSITIONS_PROPERTIES)
                .hasMessageContaining("Required key");

        assertThatAppWithProps(commonArgs, RECORDS_PROPERTIES, POSITIONS_PROPERTIES)
                .doesNotThrowAnyException();
    }

    @Test
    public void testExporterProfile() throws Exception {
        commonArgs.add("spring.profiles.active=exporter");

        assertThatAppWithProps(commonArgs)
                .hasMessageContaining("Required key");

        assertThatAppWithProps(commonArgs, POSITIONS_PROPERTIES)
                .doesNotThrowAnyException();
    }

    @Test
    public void testGatewayProfile() throws Exception {
        commonArgs.add("spring.profiles.active=gateway");

        assertThatAppWithProps(commonArgs)
                .hasMessageContaining("Required key");

        assertThatAppWithProps(commonArgs, RECORDS_PROPERTIES)
                .hasMessageContaining("Required key");

        assertThatAppWithProps(commonArgs, POSITIONS_PROPERTIES)
                .hasMessageContaining("Required key");

        assertThatAppWithProps(commonArgs, RECORDS_PROPERTIES, POSITIONS_PROPERTIES)
                .doesNotThrowAnyException();
    }

    @SafeVarargs
    protected final AbstractThrowableAssert<?, ? extends Throwable> assertThatAppWithProps(Set<String>... props) {
        if (lastApplicationContext != null) {
            lastApplicationContext.close();
        }

        return assertThatCode(() -> {
            var args = Stream.of(props)
                    .flatMap(Collection::stream)
                    .map(it -> "--" + it)
                    .toArray(String[]::new);

            lastApplicationContext = Application.start(args);
        });
    }

}
