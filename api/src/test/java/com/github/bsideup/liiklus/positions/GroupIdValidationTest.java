package com.github.bsideup.liiklus.positions;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GroupIdValidationTest {

    @Test
    void testWrongExplicitVersion() {
        assertThatThrownBy(
                () -> GroupId.of("test", -1)
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testWrongExplicitOptionalVersion() {
        assertThatThrownBy(
                () -> GroupId.of("test", Optional.of(-1))
        ).isInstanceOf(IllegalArgumentException.class);
    }
}
