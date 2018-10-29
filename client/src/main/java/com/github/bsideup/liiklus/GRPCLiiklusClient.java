package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc.ReactorLiiklusServiceStub;
import io.grpc.Channel;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import lombok.experimental.FieldDefaults;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class GRPCLiiklusClient implements LiiklusClient {

    @Delegate
    ReactorLiiklusServiceStub stub;

    public GRPCLiiklusClient(Channel channel) {
        this(ReactorLiiklusServiceGrpc.newReactorStub(channel));
    }
}
