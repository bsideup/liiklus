package com.github.bsideup.liiklus;

import com.github.bsideup.liiklus.protocol.*;
import com.google.protobuf.Empty;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.io.IOException;

public interface LiiklusClient extends Closeable {

    Mono<PublishReply> publish(PublishRequest message);

    Flux<SubscribeReply> subscribe(SubscribeRequest message);

    Flux<ReceiveReply> receive(ReceiveRequest message);

    Mono<Empty> ack(AckRequest message);

    Mono<GetOffsetsReply> getOffsets(GetOffsetsRequest message);

    @Override
    default void close() throws IOException {

    }
}
