package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.google.protobuf.Empty;
import io.rsocket.RSocket;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class RSocketLiiklusClient implements LiiklusClient {

    LiiklusServiceClient liiklusServiceClient;

    public RSocketLiiklusClient(RSocket rSocket) {
        this(new LiiklusServiceClient(rSocket));
    }

    @Override
    public Mono<PublishReply> publish(PublishRequest message) {
        return liiklusServiceClient.publish(message);
    }

    @Override
    public Flux<SubscribeReply> subscribe(SubscribeRequest message) {
        return liiklusServiceClient.subscribe(message);
    }

    @Override
    public Flux<ReceiveReply> receive(ReceiveRequest message) {
        return liiklusServiceClient.receive(message);
    }

    @Override
    public Mono<Empty> ack(AckRequest message) {
        return liiklusServiceClient.ack(message);
    }

    @Override
    public Mono<GetOffsetsReply> getOffsets(GetOffsetsRequest message) {
        return liiklusServiceClient.getOffsets(message);
    }

    @Override
    public Mono<GetEndOffsetsReply> getEndOffsets(GetEndOffsetsRequest message) {
        return liiklusServiceClient.getEndOffsets(message);
    }
}
