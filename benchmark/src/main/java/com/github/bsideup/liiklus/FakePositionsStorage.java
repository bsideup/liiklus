package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.google.auto.service.AutoService;
import org.reactivestreams.Publisher;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@AutoService(ApplicationContextInitializer.class)
public class FakePositionsStorage implements PositionsStorage, ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        if (!"FAKE".equals(applicationContext.getEnvironment().getProperty("storage.positions.type"))) {
            return;
        }
        applicationContext.registerBean(FakePositionsStorage.class);
    }

    @Override
    public CompletionStage<Void> update(String topic, GroupId groupId, int partition, long position) {
        return CompletableFuture.failedFuture(new IllegalStateException("Unsupported"));
    }

    @Override
    public Publisher<Positions> findAll() {
        return Mono.empty();
    }

    @Override
    public CompletionStage<Map<Integer, Long>> findAll(String topic, GroupId groupId) {
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }

    @Override
    public CompletionStage<Map<Integer, Map<Integer, Long>>> findAllVersionsByGroup(String topic, String groupName) {
        return CompletableFuture.completedFuture(Collections.emptyMap());
    }
}
