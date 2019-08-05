package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

import java.util.concurrent.CompletableFuture;

public class ConsumerImplAccessor {

    public static CompletableFuture<MessageId> getLastMessageIdAsync(Consumer<?> consumer) {
        return ((ConsumerImpl<?>) consumer).getLastMessageIdAsync();
    }
}
