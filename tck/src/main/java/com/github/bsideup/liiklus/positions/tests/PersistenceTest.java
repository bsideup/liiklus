package com.github.bsideup.liiklus.positions.tests;

import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorageTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public interface PersistenceTest extends PositionsStorageTestSupport {


    @Test
    @DisplayName("Should save new position")
    default void shouldSavePosition() {
        var topic = UUID.randomUUID().toString();
        var groupId = GroupId.ofString(UUID.randomUUID().toString());

        await(getStorage().update(topic, groupId, 2, 2));
        Map<Integer, Long> positions = await(getStorage().findAll(topic, groupId));

        assertThat(positions).containsEntry(2, 2L);
    }


    @Test
    @DisplayName("Should update existing position")
    default void shouldUpdatePosition() {
        var topic = UUID.randomUUID().toString();
        var groupId = GroupId.ofString(UUID.randomUUID().toString());

        await(getStorage().update(topic, groupId, 2, 2));
        await(getStorage().update(topic, groupId, 2, 3));
        Map<Integer, Long> positions = await(getStorage().findAll(topic, groupId));

        assertThat(positions)
                .containsKeys(2)
                .containsEntry(2, 3L);
    }

}
