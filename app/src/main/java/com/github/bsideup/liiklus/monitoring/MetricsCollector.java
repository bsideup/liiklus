package com.github.bsideup.liiklus.monitoring;

import com.github.bsideup.liiklus.config.ExporterProfile;
import com.github.bsideup.liiklus.positions.PositionsStorage;
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
import java.util.List;

@Component
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true)
@Slf4j
@ExporterProfile
public class MetricsCollector extends Collector {

    CollectorRegistry collectorRegistry;

    PositionsStorage positionsStorage;

    @PostConstruct
    public void init() {
        // TODO use MicroMeter abstractions
        collectorRegistry.register(this);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return Flux.from(positionsStorage.findAll())
                .<MetricFamilySamples>map(positions -> {
                    val gauge = new GaugeMetricFamily("liiklus_topic_position", "", Arrays.asList("topic", "groupId", "partition"));

                    for (val entry : positions.getValues().entrySet()) {
                        gauge.addMetric(Arrays.asList(positions.getTopic(), positions.getGroupId(), entry.getKey().toString()), entry.getValue().doubleValue());
                    }

                    return gauge;
                })
                .collectList()
                .block();
    }
}
