package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.github.bsideup.liiklus.service.ReactorLiiklusServiceImpl;
import com.google.protobuf.Empty;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class LiiklusClientAdapter implements LiiklusClient {

    ReactorLiiklusServiceImpl reactorLiiklusService;

    @Override
    public Mono<PublishReply> publish(PublishRequest message) {
        return reactorLiiklusService.publish(message, null);
    }

    @Override
    public Flux<SubscribeReply> subscribe(SubscribeRequest message) {
        return reactorLiiklusService.subscribe(message, null).hide();
    }

    @Override
    public Flux<ReceiveReply> receive(ReceiveRequest message) {
        return reactorLiiklusService.receive(message, null).hide();
    }

    @Override
    public Mono<Empty> ack(AckRequest message) {
        return reactorLiiklusService.ack(message, null);
    }

    @Override
    public Mono<GetOffsetsReply> getOffsets(GetOffsetsRequest message) {
        return reactorLiiklusService.getOffsets(message, null);
    }
}
