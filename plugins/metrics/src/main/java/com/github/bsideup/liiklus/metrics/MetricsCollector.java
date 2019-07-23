package com.github.bsideup.liiklus.metrics;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorage.Positions;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.GaugeMetricFamily;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.Comparator;

@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
public class MetricsCollector {

    PositionsStorage positionsStorage;

    public Flux<MetricFamilySamples> collect() {
        return Flux.from(positionsStorage.findAll())
                .groupBy(it -> it.getGroupId().getName())
                .flatMap(it -> it
                        .sort(Comparator.comparing(Positions::getGroupId).reversed())
                        .index()
                )
                .map(tuple -> {
                    var isLatest = tuple.getT1() == 0;
                    var positions = tuple.getT2();

                    var gauge = new GaugeMetricFamily("liiklus_topic_position", "", Arrays.asList("topic", "groupName", "groupVersion", "isLatest", "partition"));

                    for (var entry : positions.getValues().entrySet()) {
                        gauge.addMetric(
                                Arrays.asList(
                                        positions.getTopic(),
                                        positions.getGroupId().getName(),
                                        Integer.toString(positions.getGroupId().getVersion().orElse(0)),
                                        Boolean.toString(isLatest),
                                        entry.getKey().toString()
                                ),
                                entry.getValue().doubleValue()
                        );
                    }

                    return gauge;
                });
    }
}
