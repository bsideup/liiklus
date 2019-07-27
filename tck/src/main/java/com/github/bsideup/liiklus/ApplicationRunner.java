package com.github.bsideup.liiklus;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Map;

@RequiredArgsConstructor
public class ApplicationRunner {

    @NonNull
    final String recordsStorageType;

    @NonNull
    final String positionsStorageType;

    public ConfigurableApplicationContext run() {
        System.setProperty("plugins.dir", "../../plugins");
        System.setProperty("plugins.pathMatcher", "*/build/libs/*.jar");

        var application = Application.createSpringApplication(new String[0]);
        application.setDefaultProperties(Map.of(
                "server.port", 0,
                "rsocket.enabled", false,
                "grpc.enabled", false,
                "storage.records.type", recordsStorageType,
                "storage.positions.type", positionsStorageType
        ));
        return application.run();
    }
}
