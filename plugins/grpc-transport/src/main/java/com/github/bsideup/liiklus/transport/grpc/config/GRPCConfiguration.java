package com.github.bsideup.liiklus.transport.grpc.config;

import com.github.bsideup.liiklus.transport.grpc.GRPCLiiklusService;
import com.google.auto.service.AutoService;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Data;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

@AutoService(ApplicationContextInitializer.class)
public class GRPCConfiguration implements ApplicationContextInitializer<GenericApplicationContext> {

    @Override
    public void initialize(GenericApplicationContext applicationContext) {
        var environment = applicationContext.getEnvironment();

        if (!environment.acceptsProfiles(Profiles.of("gateway"))) {
            return;
        }

        var binder = Binder.get(environment);

        var serverProperties = binder.bind("grpc", GRpcServerProperties.class).orElseGet(GRpcServerProperties::new);

        if (!serverProperties.isEnabled()) {
            return;
        }

        applicationContext.registerBean(GRPCLiiklusService.class);

        applicationContext.registerBean(
                Server.class,
                () -> {
                    var serverBuilder = NettyServerBuilder
                            .forPort(serverProperties.getPort())
                            .workerEventLoopGroup(new NioEventLoopGroup(Schedulers.DEFAULT_POOL_SIZE))
                            .permitKeepAliveTime(150, TimeUnit.SECONDS)
                            .permitKeepAliveWithoutCalls(true)
                            .directExecutor()
                            .intercept(new ServerInterceptor() {
                                @Override
                                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                                    call.setCompression("gzip");
                                    return next.startCall(call, headers);
                                }
                            })
                            .addService(ProtoReflectionService.newInstance());

                    for (var bindableService : applicationContext.getBeansOfType(BindableService.class).values()) {
                        serverBuilder.addService(bindableService);
                    }

                    return serverBuilder.build();
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

    }

}
