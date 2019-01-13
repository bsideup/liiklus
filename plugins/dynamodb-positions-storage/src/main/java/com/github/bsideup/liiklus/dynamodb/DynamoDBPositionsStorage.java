package com.github.bsideup.liiklus.dynamodb;

import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.utils.ImmutableMap;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.BEGINS_WITH;
import static software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.EQ;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
public class DynamoDBPositionsStorage implements PositionsStorage {

    public static final String HASH_KEY_FIELD = "topic";

    public static final String RANGE_KEY_FIELD = "groupId";

    DynamoDbAsyncClient dynamoDB;

    String tableName;

    @Override
    public Publisher<Positions> findAll() {
        return Flux.from(dynamoDB.scanPaginator(req -> req.tableName(tableName)))
                .flatMapIterable(ScanResponse::items)
                .map(item -> new Positions(
                        item.get("topic").s(),
                        GroupId.ofString(item.get("groupId").s()),
                        toPositions(item)
                ));
    }

    @Override
    public CompletionStage<Map<Integer, Long>> findAll(String topic, GroupId groupId) {
        var request = GetItemRequest.builder()
                .tableName(tableName)
                .consistentRead(true)
                .key(toKey(topic, groupId))
                .build();

        return Mono.fromCompletionStage(() -> dynamoDB.getItem(request))
                .<Map<Integer, Long>>handle((result, sink) -> {
                    try {
                        var positions = toPositions(result.item());

                        if (positions == null) {
                            sink.complete();
                        } else {
                            sink.next(positions);
                        }
                    } catch (Exception e) {
                        sink.error(e);
                    }
                })
                .log(this.getClass().getName(), Level.WARNING, SignalType.ON_ERROR)
                .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)))
                .toFuture();
    }

    @Override
    public CompletionStage<Map<Integer, Map<Integer, Long>>> findAllVersionsByGroup(String topic, String groupName) {
        var request = QueryRequest.builder()
                .tableName(tableName)
                .keyConditions(ImmutableMap.of(
                        HASH_KEY_FIELD, condition(EQ, attribute(topic)),
                        RANGE_KEY_FIELD, condition(BEGINS_WITH, attribute(groupName))
                ))
                .build();

        return Flux
                .from(dynamoDB.queryPaginator(request))
                .flatMapIterable(QueryResponse::items)
                .map(item -> new AbstractMap.SimpleEntry<>(
                        GroupId.ofString(item.get("groupId").s()),
                        toPositions(item)
                ))
                .filter(it -> groupName.equals(it.getKey().getName()))
                .collectMap(
                        it -> it.getKey().getVersion().orElse(0),
                        Map.Entry::getValue
                )
                .toFuture();
    }

    @Override
    public CompletionStage<Void> update(String topic, GroupId groupId, int partition, long position) {
        Map<String, AttributeValue> key = toKey(topic, groupId);

        var update = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_exists(positions)")
                .updateExpression("SET positions.#p = :pos")
                .expressionAttributeNames(ImmutableMap.of(
                        "#p", Integer.toString(partition)
                ))
                .expressionAttributeValues(ImmutableMap.of(
                        ":pos", AttributeValue.builder().n(Long.toString(position)).build()
                ))
                .build();

        return Mono.fromCompletionStage(() -> dynamoDB.updateItem(update))
                .then()
                .onErrorMap(CompletionException.class, CompletionException::getCause)
                .retryWhen(it -> it.delayUntil(e -> {
                    if (!(e instanceof ConditionalCheckFailedException)) {
                        return Mono.error(e);
                    }

                    // TODO find out how to do field UPSERT with DynamoDB's expressions
                    var create = UpdateItemRequest.builder()
                            .tableName(tableName)
                            .key(key)
                            .attributeUpdates(singletonMap(
                                    "positions",
                                    AttributeValueUpdate.builder()
                                            .action(AttributeAction.PUT)
                                            .value(AttributeValue.builder().m(emptyMap()).build())
                                            .build()
                            ))
                            .expected(singletonMap("positions", ExpectedAttributeValue.builder().exists(false).build()))
                            .build();

                    return Mono.fromCompletionStage(() -> dynamoDB.updateItem(create))
                            .map(__ -> true)
                            .onErrorMap(CompletionException.class, CompletionException::getCause)
                            .onErrorResume(ConditionalCheckFailedException.class, __ -> Mono.just(true));
                }))
                .log(this.getClass().getName(), Level.WARNING, SignalType.ON_ERROR)
                .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)))
                .toFuture();
    }

    static Map<String, AttributeValue> toKey(String topic, GroupId groupId) {
        var result = new HashMap<String, AttributeValue>();
        result.put(HASH_KEY_FIELD, attribute(topic));
        result.put(RANGE_KEY_FIELD, attribute(groupId.asString()));
        return result;
    }

    static Map<Integer, Long> toPositions(Map<String, AttributeValue> item) {
        if (item == null) {
            return null;
        }

        AttributeValue positions = item.get("positions");

        if (positions == null || positions.m() == null) {
            return null;
        }

        return positions.m().entrySet().stream()
                .collect(Collectors.toMap(
                        it -> Integer.parseInt(it.getKey()),
                        it -> Long.parseLong(it.getValue().n())
                ));
    }

    static Condition condition(ComparisonOperator eq, AttributeValue attribute) {
        return Condition.builder().comparisonOperator(eq).attributeValueList(attribute).build();
    }

    static AttributeValue attribute(String topic) {
        return AttributeValue.builder().s(topic).build();
    }
}
