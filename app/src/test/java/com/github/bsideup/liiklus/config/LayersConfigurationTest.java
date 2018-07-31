package com.github.bsideup.liiklus.config;

import com.github.bsideup.liiklus.config.LayersConfiguration.LayersProperties;
import com.github.bsideup.liiklus.records.RecordPostProcessor;
import com.github.bsideup.liiklus.records.RecordPreProcessor;
import com.github.bsideup.liiklus.records.RecordsStorage;
import com.google.common.collect.ImmutableMap;
import lombok.ToString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class LayersConfigurationTest {

    @Mock
    ApplicationContext applicationContext;

    @Mock
    LayersProperties layersProperties;

    @InjectMocks
    LayersConfiguration layersConfiguration;

    @Before
    public void setUp() throws Exception {
        Mockito.when(applicationContext.getBeansOfType(RecordPreProcessor.class)).thenReturn(ImmutableMap.of(
                "p1", P1.INSTANCE,
                "p2", P2.INSTANCE,
                "p3", P3.INSTANCE
        ));
        Mockito.when(applicationContext.getBeansOfType(RecordPostProcessor.class)).thenReturn(ImmutableMap.of(
                "p1", P1.INSTANCE,
                "p2", P2.INSTANCE,
                "p3", P3.INSTANCE
        ));
    }

    @Test
    public void testOrderWithEmptyOrders() throws Exception {
        assertThat(layersConfiguration.recordPreProcessorChain().getAll()).containsExactly(
                P1.INSTANCE,
                P2.INSTANCE,
                P3.INSTANCE
        );
        assertThat(layersConfiguration.recordPostProcessorChain().getAll()).containsExactly(
                P3.INSTANCE,
                P2.INSTANCE,
                P1.INSTANCE
        );
    }

    @Test
    public void testOrderWithDefaultOrder() throws Exception {
        Mockito.when(layersProperties.getOrders()).thenReturn(ImmutableMap.of(
                P2.class.getName(), 0
        ));
        assertThat(layersConfiguration.recordPreProcessorChain().getAll()).containsExactly(
                P1.INSTANCE,
                P2.INSTANCE,
                P3.INSTANCE
        );
        assertThat(layersConfiguration.recordPostProcessorChain().getAll()).containsExactly(
                P3.INSTANCE,
                P2.INSTANCE,
                P1.INSTANCE
        );
    }

    @Test
    public void testNegativeOrders() throws Exception {
        Mockito.when(layersProperties.getOrders()).thenReturn(ImmutableMap.of(
                P2.class.getName(), -100,
                P3.class.getName(), -50
        ));

        assertThat(layersConfiguration.recordPreProcessorChain().getAll()).containsExactly(
                P2.INSTANCE,
                P3.INSTANCE,
                P1.INSTANCE
        );

        assertThat(layersConfiguration.recordPostProcessorChain().getAll()).containsExactly(
                P1.INSTANCE,
                P3.INSTANCE,
                P2.INSTANCE
        );
    }

    @Test
    public void testPositiveOrders() throws Exception {
        Mockito.when(layersProperties.getOrders()).thenReturn(ImmutableMap.of(
                P2.class.getName(), 100,
                P3.class.getName(), 50
        ));

        assertThat(layersConfiguration.recordPreProcessorChain().getAll()).containsExactly(
                P1.INSTANCE,
                P3.INSTANCE,
                P2.INSTANCE
        );

        assertThat(layersConfiguration.recordPostProcessorChain().getAll()).containsExactly(
                P2.INSTANCE,
                P3.INSTANCE,
                P1.INSTANCE
        );
    }

    @Test
    public void testOrderClashing() throws Exception {
        Mockito.when(layersProperties.getOrders()).thenReturn(ImmutableMap.of(
                P3.class.getName(), -100,
                P2.class.getName(), -100
        ));

        assertThat(layersConfiguration.recordPreProcessorChain().getAll()).containsExactly(
                P2.INSTANCE,
                P3.INSTANCE,
                P1.INSTANCE
        );

        assertThat(layersConfiguration.recordPostProcessorChain().getAll()).containsExactly(
                P1.INSTANCE,
                P3.INSTANCE,
                P2.INSTANCE
        );
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