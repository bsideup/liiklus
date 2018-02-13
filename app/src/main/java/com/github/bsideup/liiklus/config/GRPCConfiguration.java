package com.github.bsideup.liiklus.config;

import io.grpc.ServerBuilder;
import org.lognet.springboot.grpc.GRpcServerBuilderConfigurer;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GRPCConfiguration extends GRpcServerBuilderConfigurer {
    @Override
    public void configure(ServerBuilder<?> serverBuilder) {
        serverBuilder.directExecutor();
    }
}
