package com.github.bsideup.liiklus.positions;

import lombok.RequiredArgsConstructor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
@RequiredArgsConstructor
public class GroupIdTest {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<Object[]> data() {
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

    final String string;

    final GroupId object;

    @Test
    public void testParsing() {
        assertThat(GroupId.ofString(string)).isEqualTo(object);
    }

    @Test
    public void testStringRepresentation() {
        assertThat(object.asString()).isEqualTo(string);
    }
}
