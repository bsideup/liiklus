package com.github.bsideup.liiklus.transport.grpc;

import io.grpc.netty.NettyServerBuilder;

@FunctionalInterface
public interface GRPCLiiklusTransportConfigurer {
    void apply(NettyServerBuilder builder);
}
