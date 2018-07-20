package com.github.bsideup.liiklus.container;

import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;

import static org.junit.Assert.fail;

public class LiiklusContainerTest {
    
    static KafkaContainer kafka = new KafkaContainer();
    
    static {
        kafka.start();
    }
    
    @Test
    public void shouldStart() {
        LiiklusContainer liiklusContainer = new LiiklusContainer("0.4.5");
        liiklusContainer.withKafka(kafka);
        liiklusContainer.start();

        liiklusContainer.getEndpoint();

        URI uri = URI.create("tcp://" + liiklusContainer.getEndpoint());
        
        try (Socket __ = new Socket(uri.getHost(), uri.getPort())) {
          
        } catch (IOException e) {
          fail(e.getMessage());
        }
    }
}