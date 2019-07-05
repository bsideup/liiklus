package com.github.bsideup.liiklus.records;

import com.github.bsideup.liiklus.records.tests.BackPressureTest;
import com.github.bsideup.liiklus.records.tests.ConsumerGroupTest;
import com.github.bsideup.liiklus.records.tests.EndOffsetsTest;
import com.github.bsideup.liiklus.records.tests.PublishTest;
import com.github.bsideup.liiklus.records.tests.SubscribeTest;

public interface RecordStorageTests extends PublishTest, SubscribeTest, ConsumerGroupTest, BackPressureTest, EndOffsetsTest {
}
