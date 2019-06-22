# Liiklus
> Liiklus **[li:klus]** ("traffic" in Estonian) - RSocket/gRPC-based Gateway for the event-based systems from the ones who think that Kafka is too low-level.

## Why
* horizontally scalable **RSocket/gRPC streaming gateway**
* supports as many client languages as RSocket+gRPC do (Java, Go, C++, Python, etc...)
* reactive first
* Per-partition **backpressure-aware** sources
* at-least-once/at-most-once delivery guarantees
* **pluggable** event storage (Kafka, Pulsar, Kinesis, etc...)
* pluggable positions storage (DynamoDB, Cassandra, Redis, etc...)
* WIP: cold event storage support (S3, Minio, SQL, key/value, etc...)

## Who is using
* https://vivy.com/ - 25+ microservices, an abstraction in front of Kafka for the Shared Log Infrastructure (Event Sourcing / CQRS)

## Quick Start
The easiest (and recommended) way to run Liiklus is with Docker:
```shell
$ docker run \
    -e kafka_bootstrapServers=some.kafka.host:9092 \
    -e storage_positions_type=MEMORY \ # only for testing, DO NOT use in production
    -p 6565:6565 \
    bsideup/liiklus:$LATEST_VERSION
```
Where the latest version is:  
[![](https://img.shields.io/github/release/bsideup/liiklus.svg)](https://github.com/bsideup/liiklus/releases/latest)

Now use [LiiklusService.proto](protocol/src/main/proto/LiiklusService.proto) to generate your client.

The clients must implement the following algorithm:  
1. Subscribe to the assignments:  
    ```
    stub.subscribe(SubscribeRequest(
        topic="your-topic",
        group="your-consumer-group",
        [autoOffsetReset="earliest|latest"]
    ))
    ```
1. For every emitted reply of `Subscribe`, using the same channel, subscribe to the records:  
    ```
    stub.receive(ReceiveRequest(
        assignment=reply.getAssignment()
    ))
    ```
1. ACK records
    ```
    stub.ack(AckRequest(
        assignment=reply.getAssignment(),
        offset=record.getOffset()
    ))
    ```
    **Note 1:** If you ACK record before processing it you get at-most-once, after processing - at-least-once  
    **Note 2:** It's recommended to ACK every n-th record, or every n seconds to reduce the load on the positions storage


## Java example:
Example code using [Project Reactor](http://projectreactor.io) and [reactive-grpc](https://github.com/salesforce/reactive-grpc):
```java
var stub = ReactorLiiklusServiceGrpc.newReactorStub(channel);
stub
    .subscribe(
        SubscribeRequest.newBuilder()
            .setTopic("user-events")
            .setGroup("analytics")
            .setAutoOffsetReset(AutoOffsetReset.EARLIEST)
            .build()
    )
    .flatMap(reply -> stub
        .receive(ReceiveRequest.newBuilder().setAssignment(reply.getAssignment()).build())
        .window(1000) // ACK every 1000th records
        .concatMap(
            batch -> batch
                .map(ReceiveReply::getRecord)
                // TODO process instead of Mono.delay(), i.e. by indexing to ElasticSearch
                .concatMap(record -> Mono.delay(Duration.ofMillis(100)))
                .sample(Duration.ofSeconds(5)) // ACK every 5 seconds
                .onBackpressureLatest()
                .delayUntil(record -> stub.ack(
                    AckRequest.newBuilder()
                        .setAssignment(reply.getAssignment())
                        .setOffset(record.getOffset())
                        .build()
                )),
            1
        )
    )
    .blockLast()
```

Also check [examples/java/](examples/java/) for a complete example

## Configuration
The project is based on Spring Boot and uses [it's configuration system](https://docs.spring.io/spring-boot/docs/2.0.0.RELEASE/reference/html/boot-features-external-config.html)  
Please check [application.yml](app/src/main/resources/application.yml) for the available configuration keys.

## License

See [LICENSE](LICENSE).
