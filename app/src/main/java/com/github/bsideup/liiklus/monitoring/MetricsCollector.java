package com.github.bsideup.liiklus.monitoring;

import com.github.bsideup.liiklus.config.ExporterProfile;
import com.github.bsideup.liiklus.positions.PositionsStorage;
import com.github.bsideup.liiklus.positions.PositionsStorage.Positions;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.GaugeMetricFamily;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
@ExporterProfile
public class MetricsCollector extends Collector {

    CollectorRegistry collectorRegistry;

    PositionsStorage positionsStorage;

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    @PostConstruct
    public void init() {
        // TODO use MicroMeter abstractions
        collectorRegistry.register(this);
        initialized.set(true);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        if (!initialized.get()) {
            // CollectorRegistry will collect on "register", we want to avoid it
            return Collections.emptyList();
        }
        return Flux.from(positionsStorage.findAll())
                .groupBy(it -> it.getGroupId().getName())
                .flatMap(it -> it
                        .sort(Comparator.comparing(Positions::getGroupId).reversed())
                        .index()
                )
                .<MetricFamilySamples>map(tuple -> {
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
                })
                .collectList()
                .block();
    }
}
