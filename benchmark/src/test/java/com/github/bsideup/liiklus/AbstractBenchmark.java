package com.github.bsideup.liiklus;

import com.codahale.metrics.*;
import org.junit.After;
import org.junit.Before;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class AbstractBenchmark {

    protected final MetricRegistry metrics = new MetricRegistry();

    final ScheduledReporter reporter = Slf4jReporter.forRegistry(metrics)
            .outputTo(LoggerFactory.getLogger("metrics"))
            .shutdownExecutorOnStop(true)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    @Before
    public void setUpAbstractBenchmark() {
        reporter.start(1, 2, TimeUnit.SECONDS);
    }

    @After
    public void tearDownAbstractBenchmark() {
        reporter.stop();
        try (
                var reporter = ConsoleReporter.forRegistry(metrics)
                        .disabledMetricAttributes(Set.of(MetricAttribute.COUNT))
                        .shutdownExecutorOnStop(true)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build()
        ) {
            reporter.report();
        }
    }
}
