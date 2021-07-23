package com.github.bsideup.liiklus.positions;

import lombok.RequiredArgsConstructor;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@RequiredArgsConstructor
class GroupIdTest {

    static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"hello", GroupId.of("hello", Optional.empty())},
                {"hello-", GroupId.of("hello-", Optional.empty())},
                {"hello-v", GroupId.of("hello-v", Optional.empty())},
                {"hello-v-v", GroupId.of("hello-v-v", Optional.empty())},

                {"hello-v1", GroupId.of("hello", Optional.of(1))},
                {"hello-v100", GroupId.of("hello", Optional.of(100))},

                {"hello-v10-v5", GroupId.of("hello-v10", Optional.of(5))},

                {"hello-v-1", GroupId.of("hello-v-1", Optional.empty())},
                {"hello-v10-alpha", GroupId.of("hello-v10-alpha", Optional.empty())},
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    void testParsing(String string, GroupId object) {
        assertThat(GroupId.ofString(string)).isEqualTo(object);
    }

    @ParameterizedTest
    @MethodSource("data")
    void testStringRepresentation(String string, GroupId object) {
        assertThat(object.asString()).isEqualTo(string);
    }
}
