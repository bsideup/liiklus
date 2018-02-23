package reactor.kafka.receiver.internals;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Field;

@Slf4j
public class DefaultKafkaReceiverAccessor {

    private static final Field subscriptionsField;

    static {
        try {
            subscriptionsField = KafkaConsumer.class.getDeclaredField("subscriptions");
            subscriptionsField.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void close(DefaultKafkaReceiver kafkaReceiver) {
        kafkaReceiver.close();
    }


    @SneakyThrows
    public static void resume(DefaultKafkaReceiver kafkaReceiver, TopicPartition topicPartition) {
        val state = (SubscriptionState) subscriptionsField.get(kafkaReceiver.kafkaConsumer());

        if (state.isAssigned(topicPartition)) {
            try {
                state.resume(topicPartition);
            } catch (IllegalStateException e) {
                log.warn("Illegal state while accessing {}", topicPartition, e);
            }
        }
    }
}
