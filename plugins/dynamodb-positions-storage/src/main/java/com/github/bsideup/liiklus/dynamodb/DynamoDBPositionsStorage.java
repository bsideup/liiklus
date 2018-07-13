package com.github.bsideup.liiklus.dynamodb;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.util.ImmutableMapParameter;
import com.github.bsideup.liiklus.positions.GroupId;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
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
        val request = new ScanRequest(tableName);

        AtomicBoolean done = new AtomicBoolean(false);

        return Mono
                .<List<Map<String, AttributeValue>>>create(sink -> dynamoDB.scanAsync(request, new AsyncHandler<ScanRequest, ScanResult>() {
                    @Override
                    public void onError(Exception exception) {
                        sink.error(exception);
                    }

                    @Override
                    public void onSuccess(ScanRequest request, ScanResult scanResult) {
                        try {
                            if (scanResult.getCount() <= 0) {
                                done.set(true);
                                sink.success();
                            } else {
                                if (scanResult.getLastEvaluatedKey() == null || scanResult.getLastEvaluatedKey().isEmpty()) {
                                    done.set(true);
                                }
                                sink.success(scanResult.getItems());
                            }
                        } catch (Exception e) {
                            sink.error(e);
                        }
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
        val request = new GetItemRequest()
                .withTableName(tableName)
                .withConsistentRead(true)
                .withKey(toKey(topic, groupId));

        return Mono
                .<Map<Integer, Long>>create(sink -> {
                    val future = dynamoDB.getItemAsync(request, new AsyncHandler<GetItemRequest, GetItemResult>() {
                        @Override
                        public void onError(Exception exception) {
                            sink.error(exception);
                        }

                        @Override
                        public void onSuccess(GetItemRequest request, GetItemResult getItemResult) {
                            try {
                                val positions = toPositions(getItemResult.getItem());

                                if (positions == null) {
                                    sink.success();
                                } else {
                                    sink.success(positions);
                                }
                            } catch (Exception e) {
                                sink.error(e);
                            }
                        }
                    });

                    sink.onCancel(() -> future.cancel(true));
                })
                .log(this.getClass().getName(), Level.WARNING, SignalType.ON_ERROR)
                .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)))
                .toFuture();
    }

    @Override
    public CompletionStage<Map<Integer, Map<Integer, Long>>> findAllVersionsByGroup(String topic, String groupName) {
        val request = new QueryRequest(tableName)
                .addKeyConditionsEntry(HASH_KEY_FIELD, new Condition().withComparisonOperator(EQ).withAttributeValueList(new AttributeValue(topic)))
                .addKeyConditionsEntry(RANGE_KEY_FIELD, new Condition().withComparisonOperator(BEGINS_WITH).withAttributeValueList(new AttributeValue(groupName)));

        AtomicBoolean done = new AtomicBoolean(false);

        return Mono
                .<List<Map<String, AttributeValue>>>create(sink -> dynamoDB.queryAsync(request, new AsyncHandler<QueryRequest, QueryResult>() {
                    @Override
                    public void onError(Exception exception) {
                        sink.error(exception);
                    }

                    @Override
                    public void onSuccess(QueryRequest request, QueryResult result) {
                        try {
                            if (result.getCount() <= 0 || result.getLastEvaluatedKey() == null || result.getLastEvaluatedKey().isEmpty()) {
                                done.set(true);
                            }
                            sink.success(result.getItems());
                        } catch (Exception e) {
                            sink.error(e);
                        }
                    }
                }))
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
                .<Void>create(sink -> {
                    val request = new UpdateItemRequest()
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

                    val future = dynamoDB.updateItemAsync(request, new AsyncHandler<UpdateItemRequest, UpdateItemResult>() {
                        @Override
                        public void onError(Exception exception) {
                            sink.error(exception);
                        }

                        @Override
                        public void onSuccess(UpdateItemRequest request, UpdateItemResult updateItemResult) {
                            sink.success();
                        }
                    });

                    sink.onCancel(() -> future.cancel(true));
                })
                .retryWhen(it -> it.delayUntil(e -> {
                    if (!(e instanceof ConditionalCheckFailedException)) {
                        return Mono.error(e);
                    }

                    return Mono.create(sink -> {
                        // TODO find out how to do field UPSERT with DynamoDB's expressions
                        val request = new UpdateItemRequest()
                                .withTableName(tableName)
                                .withKey(key)
                                .withAttributeUpdates(singletonMap(
                                        "positions",
                                        new AttributeValueUpdate()
                                                .withAction(AttributeAction.PUT)
                                                .withValue(new AttributeValue().withM(emptyMap()))
                                ))
                                .withExpected(singletonMap("positions", new ExpectedAttributeValue(false)));

                        val future = dynamoDB.updateItemAsync(request, new AsyncHandler<UpdateItemRequest, UpdateItemResult>() {
                            @Override
                            public void onError(Exception exception) {
                                if (exception instanceof ConditionalCheckFailedException) {
                                    sink.success(true);
                                } else {
                                    sink.error(exception);
                                }
                            }

                            @Override
                            public void onSuccess(UpdateItemRequest request, UpdateItemResult updateItemResult) {
                                sink.success(true);
                            }
                        });

                        sink.onCancel(() -> future.cancel(true));
                    });
                }))
                .log(this.getClass().getName(), Level.WARNING, SignalType.ON_ERROR)
                .retryWhen(it -> it.delayElements(Duration.ofSeconds(1)))
                .toFuture();
    }

    Map<String, AttributeValue> toKey(String topic, GroupId groupId) {
        val result = new HashMap<String, AttributeValue>();
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
}
