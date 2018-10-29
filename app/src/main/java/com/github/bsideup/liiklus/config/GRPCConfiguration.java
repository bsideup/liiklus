package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.service.ReactorLiiklusServiceImpl;
import io.grpc.*;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Data;
import lombok.val;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

public class GRPCConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        val environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        val binder = Binder.get(environment);

        val serverProperties = binder.bind("grpc", GRpcServerProperties.class).orElseGet(GRpcServerProperties::new);

        applicationContext.registerBean(ReactorLiiklusServiceImpl.class);

        applicationContext.registerBean(
                Server.class,
                () -> {
                    ServerBuilder<?> serverBuilder;

                    if (serverProperties.isEnabled()) {
                        serverBuilder = NettyServerBuilder
                                .forPort(serverProperties.getPort())
                                .workerEventLoopGroup(new NioEventLoopGroup(Schedulers.DEFAULT_POOL_SIZE))
                                .permitKeepAliveTime(150, TimeUnit.SECONDS)
                                .permitKeepAliveWithoutCalls(true);
                    } else {
                        serverBuilder = InProcessServerBuilder.forName(serverProperties.getInProcessServerName());
                    }

                    return serverBuilder
                            .directExecutor()
                            .intercept(new ServerInterceptor() {
                                @Override
                                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                                    call.setCompression("gzip");
                                    return next.startCall(call, headers);
                                }
                            })
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

        String inProcessServerName;

    }

}
