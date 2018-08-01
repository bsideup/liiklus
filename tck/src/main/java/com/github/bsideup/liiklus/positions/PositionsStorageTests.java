package com.github.bsideup.liiklus.positions;

import lombok.val;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public interface PositionsStorageTests extends PositionsStorageTestSupport {

    @Test
    @DisplayName("Should save new position")
    default void shouldSavePosition() {
        val topic = UUID.randomUUID().toString();
        val groupId = GroupId.ofString(UUID.randomUUID().toString());

        await(getStorage().update(topic, groupId, 2, 2));
        Map<Integer, Long> positions = await(getStorage().findAll(topic, groupId));

        assertThat(positions).containsEntry(2, 2L);
    }


    @Test
    @DisplayName("Should update existing position")
    default void shouldUpdatePosition() {
        val topic = UUID.randomUUID().toString();
        val groupId = GroupId.ofString(UUID.randomUUID().toString());

        await(getStorage().update(topic, groupId, 2, 2));
        await(getStorage().update(topic, groupId, 2, 3));
        Map<Integer, Long> positions = await(getStorage().findAll(topic, groupId));

        assertThat(positions)
                .containsKeys(2)
                .containsEntry(2, 3L);
    }

    @Test
    @DisplayName("Should return all groups and topics")
    default void shouldReturnMultipleGroups() {
        val topic = UUID.randomUUID().toString();
        val topic2 = UUID.randomUUID().toString();
        val groupId = GroupId.ofString(UUID.randomUUID().toString());
        val groupId2 = GroupId.ofString(UUID.randomUUID().toString());

        await(getStorage().update(topic, groupId, 2, 2));
        await(getStorage().update(topic, groupId2, 2, 3));
        await(getStorage().update(topic2, groupId2, 2, 4));
        await(getStorage().update(topic2, groupId2, 3, 5));

        List<PositionsStorage.Positions> positions = Flux.from(getStorage().findAll())
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(positions)
                .filteredOn(pos -> asList(groupId, groupId2).contains(pos.getGroupId()))
                .flatExtracting(PositionsStorage.Positions::getTopic)
                .contains(topic, topic2);

        assertThat(positions)
                .filteredOn(pos -> asList(groupId, groupId2).contains(pos.getGroupId()))
                .hasSize(3)
                .flatExtracting(PositionsStorage.Positions::getValues)
                .contains(
                        mapOf(
                                2, 4L,
                                3, 5L
                        ),
                        mapOf(2, 3L),
                        mapOf(2, 2L)
                );
    }

    @Test
    @DisplayName("Should return all versions by group name")
    default void shouldReturnAllVersionsByGroup() {
        val topic = UUID.randomUUID().toString();
        val groupName = UUID.randomUUID().toString();
        val groupId1 = GroupId.of(groupName, 1);
        val groupId2 = GroupId.of(groupName, 2);

        await(getStorage().update(topic, groupId1, 2, 2));
        await(getStorage().update(topic, groupId2, 2, 3));
        await(getStorage().update(topic, groupId2, 4, 5));

        Map<Integer, Map<Integer, Long>> positions = await(getStorage().findAllVersionsByGroup(topic, groupName));

        assertThat(positions)
                .hasSize(2)
                .containsEntry(1, mapOf(2, 2L))
                .containsEntry(2, mapOf(
                        2, 3L,
                        4, 5L
                ));
    }

}
