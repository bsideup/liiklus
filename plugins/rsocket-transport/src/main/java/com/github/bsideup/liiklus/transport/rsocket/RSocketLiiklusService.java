package com.github.bsideup.liiklus.transport.rsocket;

import com.github.bsideup.liiklus.protocol.AckRequest;
import com.github.bsideup.liiklus.protocol.GetEndOffsetsReply;
import com.github.bsideup.liiklus.protocol.GetEndOffsetsRequest;
import com.github.bsideup.liiklus.protocol.GetOffsetsReply;
import com.github.bsideup.liiklus.protocol.GetOffsetsRequest;
import com.github.bsideup.liiklus.protocol.PublishReply;
import com.github.bsideup.liiklus.protocol.PublishRequest;
import com.github.bsideup.liiklus.protocol.ReceiveReply;
import com.github.bsideup.liiklus.protocol.ReceiveRequest;
import com.github.bsideup.liiklus.protocol.SubscribeReply;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.github.bsideup.liiklus.service.LiiklusService;
import com.google.protobuf.Empty;
import io.netty.buffer.ByteBuf;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class RSocketLiiklusService implements com.github.bsideup.liiklus.protocol.LiiklusService {

    LiiklusService liiklusService;

    @Override
    public Mono<PublishReply> publish(PublishRequest message, ByteBuf metadata) {
        return liiklusService.publish(Mono.just(message));
    }

    @Override
    public Flux<SubscribeReply> subscribe(SubscribeRequest message, ByteBuf metadata) {
        return liiklusService.subscribe(Mono.just(message));
    }

    @Override
    public Flux<ReceiveReply> receive(ReceiveRequest message, ByteBuf metadata) {
        return liiklusService.receive(Mono.just(message));
    }

    @Override
    public Mono<Empty> ack(AckRequest message, ByteBuf metadata) {
        return liiklusService.ack(Mono.just(message));
    }

    @Override
    public Mono<GetOffsetsReply> getOffsets(GetOffsetsRequest message, ByteBuf metadata) {
        return liiklusService.getOffsets(Mono.just(message));
    }

    @Override
    public Mono<GetEndOffsetsReply> getEndOffsets(GetEndOffsetsRequest message, ByteBuf metadata) {
        return liiklusService.getEndOffsets(Mono.just(message));
    }
}
