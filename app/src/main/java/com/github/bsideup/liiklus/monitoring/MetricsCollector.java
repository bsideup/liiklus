package com.github.bsideup.liiklus.monitoring;

import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorage.Positions;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.GaugeMetricFamily;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
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
                    val isLatest = tuple.getT1() == 0;
                    val positions = tuple.getT2();

                    val gauge = new GaugeMetricFamily("liiklus_topic_position", "", Arrays.asList("topic", "groupName", "groupVersion", "isLatest", "partition"));

                    for (val entry : positions.getValues().entrySet()) {
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
