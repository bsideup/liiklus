package com.github.bsideup.liiklus.positions.inmemory;

import com.github.bsideup.liiklus.Application;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorageTests;
import lombok.Getter;
import org.springframework.context.ApplicationContext;

class InMemoryPositionsStorageTest implements PositionsStorageTests {

    static final ApplicationContext applicationContext;

    static {
        System.setProperty("server.port", "0");
        System.setProperty("rsocket.enabled", "false");
        System.setProperty("grpc.enabled", "false");

        System.setProperty("plugins.dir", "../../plugins");
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

        System.setProperty("storage.records.type", "MEMORY");

        System.setProperty("storage.positions.type", "MEMORY");
        applicationContext = Application.start(new String[0]);
    }

    @Getter
    PositionsStorage storage = applicationContext.getBean(PositionsStorage.class);

}