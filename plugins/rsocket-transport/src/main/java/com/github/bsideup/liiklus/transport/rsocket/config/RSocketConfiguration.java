package com.github.bsideup.liiklus.transport.rsocket.config;

import com.github.bsideup.liiklus.protocol.LiiklusService;
import com.github.bsideup.liiklus.protocol.LiiklusServiceServer;
import com.github.bsideup.liiklus.transport.rsocket.RSocketLiiklusService;
import com.github.bsideup.liiklus.util.PropertiesUtil;
import com.google.auto.service.AutoService;
import io.rsocket.RSocketFactory;
import io.rsocket.rpc.rsocket.RequestHandlingRSocket;
import io.rsocket.transport.netty.server.CloseableChannel;
import io.rsocket.transport.netty.server.TcpServerTransport;
import lombok.Data;
import org.hibernate.validator.group.GroupSequenceProvider;
import org.hibernate.validator.spi.group.DefaultGroupSequenceProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import reactor.core.publisher.Mono;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AutoService(ApplicationContextInitializer.class)
public class RSocketConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        var serverProperties = PropertiesUtil.bind(environment, new RSocketServerProperties());

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

    @ConfigurationProperties("rsocket")
    @Data
    @GroupSequenceProvider(RSocketServerProperties.EnabledSequenceProvider.class)
    static class RSocketServerProperties {

        boolean enabled = true;

        @NotEmpty(groups = Enabled.class)
        String host;

        @Min(value = 0, groups = Enabled.class)
        int port = -1;

        interface Enabled {}

        public static class EnabledSequenceProvider implements DefaultGroupSequenceProvider<RSocketServerProperties> {

            @Override
            public List<Class<?>> getValidationGroups(RSocketServerProperties object) {
                var sequence = new ArrayList<Class<?>>();
                sequence.add(RSocketServerProperties.class);
                if (object != null && object.isEnabled()) {
                    sequence.add(Enabled.class);
                }
                return sequence;
            }
        }

    }
}
