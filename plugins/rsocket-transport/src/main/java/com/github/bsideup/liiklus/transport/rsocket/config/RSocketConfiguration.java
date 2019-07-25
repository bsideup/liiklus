package com.github.bsideup.liiklus.transport.rsocket.config;

import com.github.bsideup.liiklus.protocol.LiiklusService;
import com.github.bsideup.liiklus.protocol.LiiklusServiceServer;
import com.github.bsideup.liiklus.transport.rsocket.RSocketLiiklusService;
import com.google.auto.service.AutoService;
import io.rsocket.RSocketFactory;
import io.rsocket.rpc.rsocket.RequestHandlingRSocket;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import lombok.Data;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import reactor.core.publisher.Mono;

import java.util.Optional;

@AutoService(ApplicationContextInitializer.class)
public class RSocketConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        var binder = Binder.get(environment);

        var serverProperties = binder.bind("rsocket", RSocketServerProperties.class).orElseGet(RSocketServerProperties::new);

        if (!serverProperties.isEnabled()) {
            return;
        }

        applicationContext.registerBean(RSocketLiiklusService.class);

        applicationContext.registerBean(
                CloseableChannel.class,
                () -> {
                    var liiklusService = applicationContext.getBean(LiiklusService.class);

                    return RSocketFactory.receive()
                            .acceptor((setup, sendingSocket) -> Mono.just(new RequestHandlingRSocket(new LiiklusServiceServer(liiklusService, Optional.empty(), Optional.empty()))))
                            .transport(TcpServerTransport.create(serverProperties.getHost(), serverProperties.getPort()))
                            .start()
                            .block();
                },
                it -> {
                    it.setDestroyMethodName("dispose");
                }
        );
    }


    @Data
    static class RSocketServerProperties {

        String host = "0.0.0.0";

        int port = 8081;

        boolean enabled = true;

    }
}
