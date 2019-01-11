package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.service.ReactorLiiklusServiceImpl;
import io.grpc.*;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import lombok.Data;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;

import java.util.concurrent.TimeUnit;

public class GRPCConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        var binder = Binder.get(environment);

        var serverProperties = binder.bind("grpc", GRpcServerProperties.class).orElseGet(GRpcServerProperties::new);

        applicationContext.registerBean(ReactorLiiklusServiceImpl.class);

        applicationContext.registerBean(
                Server.class,
                () -> {
                    ServerBuilder<?> serverBuilder;

                    if (serverProperties.isEnabled()) {
                        serverBuilder = NettyServerBuilder
                                .forPort(serverProperties.getPort())
                                .permitKeepAliveTime(150, TimeUnit.SECONDS)
                                .permitKeepAliveWithoutCalls(true);
                    } else {
                        serverBuilder = InProcessServerBuilder.forName(serverProperties.getInProcessServerName());
                    }

                    var compression = serverProperties.getCompression();
                    if (compression != null) {
                        serverBuilder.intercept(new ServerInterceptor() {
                            @Override
                            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                                call.setCompression(compression);
                                return next.startCall(call, headers);
                            }
                        });
                    }

                    if (serverProperties.isDirectExecutor()) {
                        serverBuilder.directExecutor();
                    }

                    return serverBuilder
                            .addService(applicationContext.getBean(ReactorLiiklusServiceImpl.class))
                            .build();
                },
                it -> {
                    it.setInitMethodName("start");
                    it.setDestroyMethodName("shutdownNow");
                }
        );
    }

    @Data
    static class GRpcServerProperties {

        int port = 6565;

        boolean enabled = true;

        boolean directExecutor = false;

        String compression = null;

        String inProcessServerName;

    }

}
