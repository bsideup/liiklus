package com.github.bsideup.liiklus.transport.grpc.config;

import com.github.bsideup.liiklus.transport.grpc.GRPCLiiklusService;
import com.google.auto.service.AutoService;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Data;
import org.hibernate.validator.group.GroupSequenceProvider;
import org.hibernate.validator.spi.group.DefaultGroupSequenceProvider;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Profiles;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;
import reactor.core.scheduler.Schedulers;

import javax.validation.Validation;
import javax.validation.constraints.Min;
import java.util.ArrayList;
import java.util.List;
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

        var validationBindHandler = new ValidationBindHandler(
                new SpringValidatorAdapter(Validation.buildDefaultValidatorFactory().getValidator())
        );
        var serverProperties = binder.bind("grpc", Bindable.of(GRpcServerProperties.class), validationBindHandler)
                .orElseGet(GRpcServerProperties::new);

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
    @GroupSequenceProvider(GRpcServerProperties.EnabledSequenceProvider.class)
    static class GRpcServerProperties {

        boolean enabled = true;

        @Min(value = 0, groups = Enabled.class)
        int port = -1;

        interface Enabled {}

        public static class EnabledSequenceProvider implements DefaultGroupSequenceProvider<GRpcServerProperties> {

            @Override
            public List<Class<?>> getValidationGroups(GRpcServerProperties object) {
                var sequence = new ArrayList<Class<?>>();
                sequence.add(GRpcServerProperties.class);
                if (object != null && object.isEnabled()) {
                    sequence.add(Enabled.class);
                }
                return sequence;
            }
        }

    }

}
