package com.github.bsideup.liiklus.positions;

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
        GroupId groupId = GroupId.ofString(UUID.randomUUID().toString());

        Map<Integer, Long> positions = asDeferMono(() -> getStorage().update(getTopic(), groupId, 2, 2))
                .then(
                        asDeferMono(() -> getStorage().findAll(getTopic(), groupId))
                )
                .block(Duration.ofSeconds(10));

        assertThat(positions).containsEntry(2, 2L);
    }


    @Test
    @DisplayName("Should update existing position")
    default void shouldUpdatePosition() {
        GroupId groupId = GroupId.ofString(UUID.randomUUID().toString());

        Map<Integer, Long> positions = Flux
                .concat(
                        asDeferMono(() -> getStorage().update(getTopic(), groupId, 2, 2)),
                        asDeferMono(() -> getStorage().update(getTopic(), groupId, 2, 3))
                )
                .then(
                        asDeferMono(() -> getStorage().findAll(getTopic(), groupId))
                )
                .block(Duration.ofSeconds(10));

        assertThat(positions)
                .containsKeys(2)
                .containsEntry(2, 3L);
    }

    @Test
    @DisplayName("Should return all groups and topics")
    default void shouldReturnMultipleGroups() {
        GroupId groupId = GroupId.ofString(UUID.randomUUID().toString());
        GroupId groupId2 = GroupId.ofString(UUID.randomUUID().toString());

        List<PositionsStorage.Positions> positions = Flux
                .concat(
                        asDeferMono(() -> getStorage().update(getTopic(), groupId, 2, 2)),
                        asDeferMono(() -> getStorage().update(getTopic(), groupId2, 2, 3)),
                        asDeferMono(() -> getStorage().update(getTopic() + 1, groupId2, 2, 4)),
                        asDeferMono(() -> getStorage().update(getTopic() + 1, groupId2, 3, 5))
                )
                .thenMany(
                        getStorage().findAll()
                )
                .collectList()
                .block(Duration.ofSeconds(10));

        assertThat(positions)
                .filteredOn(pos -> asList(groupId, groupId2).contains(pos.getGroupId()))
                .flatExtracting(PositionsStorage.Positions::getTopic)
                .contains(getTopic(), getTopic() + 1);

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
        String groupName = UUID.randomUUID().toString();
        GroupId groupId1 = GroupId.of(groupName, 1);
        GroupId groupId2 = GroupId.of(groupName, 2);

        Map<Integer, Map<Integer, Long>> positions = Flux
                .concat(
                        asDeferMono(() -> getStorage().update(getTopic(), groupId1, 2, 2)),
                        asDeferMono(() -> getStorage().update(getTopic(), groupId2, 2, 3)),
                        asDeferMono(() -> getStorage().update(getTopic(), groupId2, 4, 5))
                )
                .then(
                        asDeferMono(() -> getStorage().findAllVersionsByGroup(getTopic(), groupName))
                )
                .block(Duration.ofSeconds(10));

        assertThat(positions)
                .hasSize(2)
                .containsEntry(1, mapOf(2, 2L))
                .containsEntry(2, mapOf(
                        2, 3L,
                        4, 5L
                ));
    }

}
