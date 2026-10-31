package org.pangrova.observability.api.event;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for the Event Listener SPI family (G-SPI-08).
 */
class EventListenerSpiTest {

    // ── QueryEventListener default methods ──

    @Test
    void defaultListenerMethodsAreNoOps() {
        QueryEventListener listener = new QueryEventListener() {};

        assertThatCode(() -> listener.queryCreated(
                new QueryCreatedEvent("q1", "SELECT 1", "admin", System.currentTimeMillis())))
                .doesNotThrowAnyException();

        assertThatCode(() -> listener.queryCompleted(
                new QueryCompletedEvent("q1", "SELECT 1", "admin",
                        QueryState.FINISHED, 100, 200,
                        QueryStatistics.wallTimeOnly(100), Optional.empty())))
                .doesNotThrowAnyException();

        assertThatCode(() -> listener.splitCompleted(
                new SplitCompletedEvent("q1", "stage-0", "split-0", "iceberg",
                        100, 200, 1000, 4096, Optional.empty())))
                .doesNotThrowAnyException();
    }

    @Test
    void customListenerReceivesAllEvents() {
        List<String> events = new ArrayList<>();

        QueryEventListener listener = new QueryEventListener() {
            @Override
            public void queryCreated(QueryCreatedEvent event) {
                events.add("created:" + event.queryId());
            }

            @Override
            public void queryCompleted(QueryCompletedEvent event) {
                events.add("completed:" + event.queryId());
            }

            @Override
            public void splitCompleted(SplitCompletedEvent event) {
                events.add("split:" + event.splitId());
            }
        };

        listener.queryCreated(
                new QueryCreatedEvent("q1", "SELECT 1", "admin", 1000));
        listener.splitCompleted(
                new SplitCompletedEvent("q1", "s0", "sp1", "iceberg",
                        1000, 1050, 500, 2048, Optional.empty()));
        listener.queryCompleted(
                new QueryCompletedEvent("q1", "SELECT 1", "admin",
                        QueryState.FINISHED, 1000, 1100,
                        new QueryStatistics(100, 50, 500, 2048), Optional.empty()));

        assertThat(events).containsExactly("created:q1", "split:sp1", "completed:q1");
    }

    // ── SplitCompletedEvent ──

    @Test
    void splitCompletedEventSucceeded() {
        SplitCompletedEvent event = new SplitCompletedEvent(
                "q1", "stage-0", "split-0", "iceberg",
                1000, 1050, 500, 4096, Optional.empty());

        assertThat(event.succeeded()).isTrue();
        assertThat(event.durationMillis()).isEqualTo(50);
        assertThat(event.processedRows()).isEqualTo(500);
        assertThat(event.processedBytes()).isEqualTo(4096);
    }

    @Test
    void splitCompletedEventFailed() {
        SplitCompletedEvent event = new SplitCompletedEvent(
                "q1", "stage-0", "split-0", "iceberg",
                1000, 1200, 0, 0, Optional.of("Connection reset"));

        assertThat(event.succeeded()).isFalse();
        assertThat(event.failureMessage()).contains("Connection reset");
        assertThat(event.durationMillis()).isEqualTo(200);
    }

    @Test
    void splitCompletedEventNullFailureDefaultsToEmpty() {
        SplitCompletedEvent event = new SplitCompletedEvent(
                "q1", "stage-0", "split-0", null,
                0, 0, 0, 0, null);

        assertThat(event.failureMessage()).isEmpty();
        assertThat(event.catalogName()).isEmpty();
        assertThat(event.succeeded()).isTrue();
    }
}
