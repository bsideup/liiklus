package reactor.kafka.receiver.internals;

import org.apache.kafka.common.TopicPartition;

public class DefaultKafkaReceiverAccessor {

    public static void updateOffset(DefaultKafkaReceiver kafkaReceiver, TopicPartition topicPartition, long offset) {
        kafkaReceiver.committableBatch().updateOffset(topicPartition, offset);
    }

    public static void close(DefaultKafkaReceiver kafkaReceiver) {
        kafkaReceiver.close();
    }
}
