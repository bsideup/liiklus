package com.github.bsideup.liiklus.records;

import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface FiniteRecordsStorage extends RecordsStorage {

    /**
     * Returns a {@link Map} where key is partition's number and value is the latest offset.
     * The offset can be zero. Offset -1 means that there is no offset for this partition.
     */
    CompletionStage<Map<Integer, Long>> getEndOffsets(String topic);
}