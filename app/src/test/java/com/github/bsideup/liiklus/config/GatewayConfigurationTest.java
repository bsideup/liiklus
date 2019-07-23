package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.records.RecordPostProcessor;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.records.RecordsStorage;
import lombok.ToString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class GatewayConfigurationTest {

    MockEnvironment environment;

    StaticApplicationContext applicationContext;

    final GatewayConfiguration gatewayConfiguration = new GatewayConfiguration();

    @Before
    public void setUp() throws Exception {
        environment = new MockEnvironment();
        environment.setActiveProfiles("gateway");

        applicationContext = new StaticApplicationContext();
        applicationContext.setEnvironment(environment);

        applicationContext.registerBean(P1.class, () -> P1.INSTANCE);
        applicationContext.registerBean(P2.class, () -> P2.INSTANCE);
        applicationContext.registerBean(P3.class, () -> P3.INSTANCE);
    }

    @Test
    public void testOrderWithEmptyOrders() throws Exception {
        gatewayConfiguration.initialize(applicationContext);

        assertThat(applicationContext.getBean(RecordPreProcessorChain.class).getAll()).containsExactly(
                P1.INSTANCE,
                P2.INSTANCE,
                P3.INSTANCE
        );
        assertThat(applicationContext.getBean(RecordPostProcessorChain.class).getAll()).containsExactly(
                P3.INSTANCE,
                P2.INSTANCE,
                P1.INSTANCE
        );
    }

    @Test
    public void testOrderWithDefaultOrder() throws Exception {
        setOrder(P2.class, 0);

        gatewayConfiguration.initialize(applicationContext);
        assertThat(applicationContext.getBean(RecordPreProcessorChain.class).getAll()).containsExactly(
                P1.INSTANCE,
                P2.INSTANCE,
                P3.INSTANCE
        );
        assertThat(applicationContext.getBean(RecordPostProcessorChain.class).getAll()).containsExactly(
                P3.INSTANCE,
                P2.INSTANCE,
                P1.INSTANCE
        );
    }

    @Test
    public void testNegativeOrders() throws Exception {
        setOrder(P2.class, -100);
        setOrder(P3.class, -50);
        gatewayConfiguration.initialize(applicationContext);

        assertThat(applicationContext.getBean(RecordPreProcessorChain.class).getAll()).containsExactly(
                P2.INSTANCE,
                P3.INSTANCE,
                P1.INSTANCE
        );

        assertThat(applicationContext.getBean(RecordPostProcessorChain.class).getAll()).containsExactly(
                P1.INSTANCE,
                P3.INSTANCE,
                P2.INSTANCE
        );
    }

    @Test
    public void testPositiveOrders() throws Exception {
        setOrder(P2.class, 100);
        setOrder(P3.class, 50);
        gatewayConfiguration.initialize(applicationContext);

        assertThat(applicationContext.getBean(RecordPreProcessorChain.class).getAll()).containsExactly(
                P1.INSTANCE,
                P3.INSTANCE,
                P2.INSTANCE
        );

        assertThat(applicationContext.getBean(RecordPostProcessorChain.class).getAll()).containsExactly(
                P2.INSTANCE,
                P3.INSTANCE,
                P1.INSTANCE
        );
    }

    @Test
    public void testOrderClashing() throws Exception {
        setOrder(P3.class, -100);
        setOrder(P2.class, -100);
        gatewayConfiguration.initialize(applicationContext);

        assertThat(applicationContext.getBean(RecordPreProcessorChain.class).getAll()).containsExactly(
                P2.INSTANCE,
                P3.INSTANCE,
                P1.INSTANCE
        );

        assertThat(applicationContext.getBean(RecordPostProcessorChain.class).getAll()).containsExactly(
                P1.INSTANCE,
                P3.INSTANCE,
                P2.INSTANCE
        );
    }

    private void setOrder(Class<?> layer, int order) {
        environment.setProperty("layers.orders[" + layer.getName() + "]", order + "");
    }

    @ToString
    private enum P1 implements RecordPreProcessor, RecordPostProcessor {
        INSTANCE;

        @Override
        public CompletionStage<RecordsStorage.Envelope> preProcess(RecordsStorage.Envelope envelope) {
            return null;
        }

        @Override
        public Publisher<RecordsStorage.Record> postProcess(Publisher<RecordsStorage.Record> records) {
            return null;
        }
    }

    @ToString
    private enum P2 implements RecordPreProcessor, RecordPostProcessor {
        INSTANCE;

        @Override
        public CompletionStage<RecordsStorage.Envelope> preProcess(RecordsStorage.Envelope envelope) {
            return null;
        }

        @Override
        public Publisher<RecordsStorage.Record> postProcess(Publisher<RecordsStorage.Record> records) {
            return null;
        }
    }

    @ToString
    private enum P3 implements RecordPreProcessor, RecordPostProcessor {
        INSTANCE;

        @Override
        public CompletionStage<RecordsStorage.Envelope> preProcess(RecordsStorage.Envelope envelope) {
            return null;
        }

        @Override
        public Publisher<RecordsStorage.Record> postProcess(Publisher<RecordsStorage.Record> records) {
            return null;
        }
    }
}