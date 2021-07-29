package com.github.bsideup.liiklus;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class ApplicationRunnerTest {

    @Nested
    class DirectLaunch {
        @Test
        void shouldStart() {
            try (var context = new ApplicationRunner("MEMORY", "MEMORY").run()) {
                assertThat(context).isNotNull();
            }
        }
    }


    @Nested
    class ExtensionLaunch {
        @RegisterExtension
        ApplicationRunner app = new ApplicationRunner("MEMORY", "MEMORY");

        @Test
        void shouldStart(ConfigurableApplicationContext ctx) {
            assertThat(ctx).isNotNull();
        }
    }
}