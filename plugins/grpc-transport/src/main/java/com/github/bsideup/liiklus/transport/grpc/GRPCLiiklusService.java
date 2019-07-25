package com.github.bsideup.liiklus.transport.grpc;

import com.github.bsideup.liiklus.protocol.AckRequest;
import com.github.bsideup.liiklus.protocol.GetEndOffsetsReply;
import com.github.bsideup.liiklus.protocol.GetEndOffsetsRequest;
import com.github.bsideup.liiklus.protocol.GetOffsetsReply;
import com.github.bsideup.liiklus.protocol.GetOffsetsRequest;
import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReactorLiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.github.bsideup.liiklus.service.LiiklusService;
import com.google.protobuf.Empty;
import io.grpc.Status;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class GRPCLiiklusService extends ReactorLiiklusServiceGrpc.LiiklusServiceImplBase {

    LiiklusService liiklusService;

    @Override
    public Mono<PublishReply> publish(Mono<PublishRequest> request) {
        return liiklusService.publish(request)
                .onErrorMap(e -> Status.INTERNAL.withCause(e).withDescription(e.getMessage()).asException());
    }

    @Override
    public Flux<SubscribeReply> subscribe(Mono<SubscribeRequest> request) {
        return liiklusService.subscribe(request)
                .onErrorMap(e -> Status.INTERNAL.withCause(e).withDescription(e.getMessage()).asException());
    }

    @Override
    public Flux<ReceiveReply> receive(Mono<ReceiveRequest> request) {
        return liiklusService.receive(request)
                .onErrorMap(e -> Status.INTERNAL.withCause(e).withDescription(e.getMessage()).asException());
    }

    @Override
    public Mono<Empty> ack(Mono<AckRequest> request) {
        return liiklusService.ack(request)
                .onErrorMap(e -> Status.INTERNAL.withCause(e).withDescription(e.getMessage()).asException());
    }

    @Override
    public Mono<GetOffsetsReply> getOffsets(Mono<GetOffsetsRequest> request) {
        return liiklusService.getOffsets(request)
                .onErrorMap(e -> Status.INTERNAL.withCause(e).withDescription(e.getMessage()).asException());
    }

    @Override
    public Mono<GetEndOffsetsReply> getEndOffsets(Mono<GetEndOffsetsRequest> request) {
        return liiklusService.getEndOffsets(request)
                .onErrorMap(e -> Status.INTERNAL.withCause(e).withDescription(e.getMessage()).asException());
    }

}
