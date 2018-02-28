package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.service.ReactorLiiklusServiceImpl;
import com.linecorp.armeria.server.PathMapping;
import com.linecorp.armeria.server.Service;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.tomcat.TomcatService;
import com.linecorp.armeria.spring.ArmeriaServerConfigurator;
import lombok.val;
import org.apache.catalina.connector.Connector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.EmbeddedWebApplicationContext;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

@Configuration
public class ArmeriaConfiguration {

    @Autowired
    ReactorLiiklusServiceImpl liiklusService;

    @Bean
    ArmeriaServerConfigurator serviceInitializer(EmbeddedWebApplicationContext applicationContext) {
        return builder -> {
            val container = (TomcatEmbeddedServletContainer) applicationContext.getEmbeddedServletContainer();
            Connector tomcatConnector = container.getTomcat().getConnector();
            if (tomcatConnector == null) {
                try {
                    val serviceConnectorsField = TomcatEmbeddedServletContainer.class.getDeclaredField("serviceConnectors");
                    serviceConnectorsField.setAccessible(true);
                    tomcatConnector = ((Map<Service, Connector[]>) serviceConnectorsField.get(container)).values()
                            .stream()
                            .flatMap(Stream::of)
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("Connectors not found"));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            builder
                    .idleTimeout(Duration.ofSeconds(180))
                    .service(new GrpcServiceBuilder().addService(liiklusService).build())
                    .service(PathMapping.ofCatchAll(), TomcatService.forConnector(tomcatConnector));
        };
    }
}
