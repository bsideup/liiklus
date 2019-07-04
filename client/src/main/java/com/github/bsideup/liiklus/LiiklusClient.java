package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.google.protobuf.Empty;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface LiiklusClient {

    Mono<PublishReply> publish(PublishRequest message);

    Flux<SubscribeReply> subscribe(SubscribeRequest message);

    Flux<ReceiveReply> receive(ReceiveRequest message);

    Mono<Empty> ack(AckRequest message);

    Mono<GetOffsetsReply> getOffsets(GetOffsetsRequest message);

    Mono<GetEndOffsetsReply> getEndOffsets(GetEndOffsetsRequest message);
}
