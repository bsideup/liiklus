package com.github.bsideup.liiklus.config;

import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import org.lognet.springboot.grpc.GRpcServerBuilderConfigurer;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.TimeUnit;

@Configuration
@GatewayProfile
public class GRPCConfiguration extends GRpcServerBuilderConfigurer {
    @Override
    public void configure(ServerBuilder<?> serverBuilder) {
        serverBuilder.directExecutor();

        if (serverBuilder instanceof NettyServerBuilder) {
            ((NettyServerBuilder) serverBuilder)
                    .workerEventLoopGroup(new NioEventLoopGroup(Schedulers.DEFAULT_POOL_SIZE))
                    .permitKeepAliveTime(150, TimeUnit.SECONDS)
                    .permitKeepAliveWithoutCalls(true);
        }
    }
}
