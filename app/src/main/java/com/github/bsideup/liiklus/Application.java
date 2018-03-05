package com.github.bsideup.liiklus;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Collections;

@Slf4j
@SpringBootApplication
public class Application {

    public static void main(String[] args) throws Exception {
        start(args);
    }

    public static ConfigurableApplicationContext start(String[] args) {
        val application = new SpringApplication(Application.class);
        application.setDefaultProperties(Collections.singletonMap("spring.profiles.active", "exporter,gateway"));

        return application.run(args);
    }
}
