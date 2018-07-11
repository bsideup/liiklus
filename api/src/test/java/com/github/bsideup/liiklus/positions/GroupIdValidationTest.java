package com.github.bsideup.liiklus.positions;

import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GroupIdValidationTest {

    @Test
    public void testWrongExplicitVersion() {
        assertThatThrownBy(
                () -> GroupId.of("test", -1)
        ).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testWrongExplicitOptionalVersion() {
        assertThatThrownBy(
                () -> GroupId.of("test", Optional.of(-1))
        ).isInstanceOf(IllegalArgumentException.class);
    }
}
