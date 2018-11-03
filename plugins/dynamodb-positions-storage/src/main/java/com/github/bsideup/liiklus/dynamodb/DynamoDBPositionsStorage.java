package com.github.bsideup.liiklus.dynamodb;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.util.ImmutableMapParameter;
import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.SignalType;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.BEGINS_WITH;
import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
public class DynamoDBPositionsStorage implements PositionsStorage {

    public static final String HASH_KEY_FIELD = "topic";

    public static final String RANGE_KEY_FIELD = "groupId";

    AmazonDynamoDBAsync dynamoDB;

    String tableName;

    @Override
    public Publisher<Positions> findAll() {
        var request = new ScanRequest(tableName);

        AtomicBoolean done = new AtomicBoolean(false);

        return Mono
                .<ScanResult>create(sink -> dynamoDB.scanAsync(request, new DefaultAsyncHandler<>(sink)))
                .<List<Map<String, AttributeValue>>>handle(((scanResult, sink) -> {
                    try {
                        if (scanResult.getCount() <= 0) {
                            done.set(true);
                            sink.complete();
                        } else {
                            if (scanResult.getLastEvaluatedKey() == null || scanResult.getLastEvaluatedKey().isEmpty()) {
                                done.set(true);
                            }
                            sink.next(scanResult.getItems());
                        }
                    } catch (Exception e) {
                        sink.error(e);
                    }
                }))
                .repeat(() -> !done.get())
                .flatMapIterable(it -> it)
                .map(item -> new Positions(
                        item.get("topic").getS(),
                        GroupId.ofString(item.get("groupId").getS()),
                        toPositions(item)
                ));
    }

    @Override
    public CompletionStage<Map<Integer, Long>> findAll(String topic, GroupId groupId) {
        var request = new GetItemRequest()
                .withTableName(tableName)
                .withConsistentRead(true)
                .withKey(toKey(topic, groupId));

        return Mono
                .<GetItemResult>create(sink -> {
                    var future = dynamoDB.getItemAsync(request, new DefaultAsyncHandler<>(sink));
                    sink.onCancel(() -> future.cancel(true));
                })
                .<Map<Integer, Long>>handle((result, sink) -> {
                    try {
                        var positions = toPositions(result.getItem());

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
        var request = new QueryRequest(tableName)
                .addKeyConditionsEntry(HASH_KEY_FIELD, new Condition().withComparisonOperator(EQ).withAttributeValueList(new AttributeValue(topic)))
                .addKeyConditionsEntry(RANGE_KEY_FIELD, new Condition().withComparisonOperator(BEGINS_WITH).withAttributeValueList(new AttributeValue(groupName)));

        AtomicBoolean done = new AtomicBoolean(false);

        return Mono
                .<QueryResult>create(sink -> dynamoDB.queryAsync(request, new DefaultAsyncHandler<>(sink)))
                .<List<Map<String, AttributeValue>>>handle((result, sink) -> {
                    try {
                        if (result.getCount() <= 0 || result.getLastEvaluatedKey() == null || result.getLastEvaluatedKey().isEmpty()) {
                            done.set(true);
                        }
                        sink.next(result.getItems());
                    } catch (Exception e) {
                        sink.error(e);
                    }
                })
                .repeat(() -> !done.get())
                .flatMapIterable(it -> it)
                .map(item -> new AbstractMap.SimpleEntry<>(
                        GroupId.ofString(item.get("groupId").getS()),
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

        return Mono
                .<UpdateItemResult>create(sink -> {
                    var request = new UpdateItemRequest()
                            .withTableName(tableName)
                            .withKey(key)
                            .withConditionExpression("attribute_exists(positions)")
                            .withUpdateExpression("SET positions.#p = :pos")
                            .withExpressionAttributeNames(ImmutableMapParameter.of(
                                    "#p", Integer.toString(partition)
                            ))
                            .withExpressionAttributeValues(ImmutableMapParameter.of(
                                    ":pos", new AttributeValue().withN(Long.toString(position))
                            ));

                    var future = dynamoDB.updateItemAsync(request, new DefaultAsyncHandler<>(sink));

                    sink.onCancel(() -> future.cancel(true));
                })
                .then()
                .retryWhen(it -> it.delayUntil(e -> {
                    if (!(e instanceof ConditionalCheckFailedException)) {
                        return Mono.error(e);
                    }

                    return Mono
                            .<UpdateItemResult>create(sink -> {
                                // TODO find out how to do field UPSERT with DynamoDB's expressions
                                var request = new UpdateItemRequest()
                                        .withTableName(tableName)
                                        .withKey(key)
                                        .withAttributeUpdates(singletonMap(
                                                "positions",
                                                new AttributeValueUpdate()
                                                        .withAction(AttributeAction.PUT)
                                                        .withValue(new AttributeValue().withM(emptyMap()))
                                        ))
                                        .withExpected(singletonMap("positions", new ExpectedAttributeValue(false)));

                                var future = dynamoDB.updateItemAsync(request, new DefaultAsyncHandler<>(sink));

                                sink.onCancel(() -> future.cancel(true));
                            })
                            .map(__ -> true)
                            .onErrorResume(ConditionalCheckFailedException.class, __ -> Mono.just(true));
                }))
                .log(this.getClass().getName(), Level.WARNING, SignalType.ON_ERROR)
                .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)))
                .toFuture();
    }

    Map<String, AttributeValue> toKey(String topic, GroupId groupId) {
        var result = new HashMap<String, AttributeValue>();
        result.put(HASH_KEY_FIELD, new AttributeValue(topic));
        result.put(RANGE_KEY_FIELD, new AttributeValue(groupId.asString()));
        return result;
    }

    Map<Integer, Long> toPositions(Map<String, AttributeValue> item) {
        if (item == null) {
            return null;
        }

        AttributeValue positions = item.get("positions");

        if (positions == null || positions.getM() == null) {
            return null;
        }

        return positions.getM().entrySet().stream()
                .collect(Collectors.toMap(
                        it -> Integer.parseInt(it.getKey()),
                        it -> Long.parseLong(it.getValue().getN())
                ));
    }


    @AllArgsConstructor
    public static class DefaultAsyncHandler<REQUEST extends AmazonWebServiceRequest, RESULT> implements AsyncHandler<REQUEST, RESULT> {
        protected MonoSink<RESULT> sink;

        @Override
        public void onError(Exception exception) {
            sink.error(exception);
        }

        @Override
        public void onSuccess(REQUEST request, RESULT result) {
            sink.success(result);
        }
    }
}
