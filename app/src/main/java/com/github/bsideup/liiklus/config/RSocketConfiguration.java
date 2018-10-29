package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.protocol.LiiklusServiceServer;
import com.github.bsideup.liiklus.service.ReactorLiiklusServiceImpl;
import io.rsocket.RSocketFactory;
import io.rsocket.rpc.rsocket.RequestHandlingRSocket;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import lombok.Data;
import lombok.val;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import reactor.core.publisher.Mono;

import java.util.Optional;

public class RSocketConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        val environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        val binder = Binder.get(environment);

        val serverProperties = binder.bind("rsocket", RSocketServerProperties.class).orElseGet(RSocketServerProperties::new);

        if (!serverProperties.isEnabled()) {
            return;
        }

        applicationContext.registerBean(
                CloseableChannel.class,
                () -> {
                    val liiklusService = applicationContext.getBean(ReactorLiiklusServiceImpl.class);
                    return RSocketFactory.receive()
                            .acceptor((setup, sendingSocket) -> Mono.just(new RequestHandlingRSocket(new LiiklusServiceServer(liiklusService, Optional.empty(), Optional.empty()))))
                            .transport(TcpServerTransport.create(serverProperties.getPort()))
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

        int port = 8081;

        boolean enabled = true;

    }
}
