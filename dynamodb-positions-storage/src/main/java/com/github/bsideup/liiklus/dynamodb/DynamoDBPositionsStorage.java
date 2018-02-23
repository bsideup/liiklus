package com.github.bsideup.liiklus.dynamodb;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.util.ImmutableMapParameter;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.val;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.logging.Level;
import java.util.stream.Collectors;

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
    public CompletionStage<Map<Integer, Long>> fetch(String topic, String groupId, Set<Integer> partitions, Map<Integer, Long> externalPositions) {
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
                                Map<String, AttributeValue> item = getItemResult.getItem();

                                val positions = item != null ? item.get("positions").getM() : null;

                                sink.success(
                                        partitions.stream().collect(Collectors.toMap(
                                                it -> it,
                                                it -> {
                                                    AttributeValue attributeValue = positions != null ? positions.get(it.toString()) : null;

                                                    if (attributeValue != null) {
                                                        return ((BigDecimal) ItemUtils.toSimpleValue(attributeValue)).longValue();
                                                    } else {
                                                        return externalPositions.get(it);
                                                    }
                                                }
                                        ))
                                );
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
    public CompletionStage<Void> update(String topic, String groupId, int partition, long position) {
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

    Map<String, AttributeValue> toKey(String topic, String groupId) {
        val result = new HashMap<String, AttributeValue>();
        result.put(HASH_KEY_FIELD, new AttributeValue(topic));
        result.put(RANGE_KEY_FIELD, new AttributeValue(groupId));
        return result;
    }
}
