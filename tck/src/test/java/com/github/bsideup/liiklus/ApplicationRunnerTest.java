package com.github.bsideup.liiklus;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ApplicationRunnerTest {

    @Test
    void shouldStart() {
        var context = new ApplicationRunner("MEMORY", "MEMORY").run();

        assertThat(context).isNotNull();
    }

}