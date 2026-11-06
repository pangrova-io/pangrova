package org.pangrova.security.core.audit;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.api.audit.AuditEventType;
import org.pangrova.security.spi.audit.AuditEventWriter;

/**
 * Validates: Requirements 18.10 — AuditEventEmitter writes events asynchronously with fault isolation.
 */
class AuditEventEmitterTest {

    @Test
    void emitsEventToWriter() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<AuditEvent> received = new CopyOnWriteArrayList<>();
        AuditEventWriter writer = event -> {
            received.add(event);
            latch.countDown();
        };

        AuditEventEmitter emitter = new AuditEventEmitter(writer);
        try {
            AuditEvent event = createEvent("evt-1", AuditEventType.QUERY_SUBMITTED);
            emitter.emit(event);
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(received).hasSize(1);
            assertThat(received.get(0).eventId()).isEqualTo("evt-1");
        } finally {
            emitter.close();
        }
    }

    @Test
    void isolatesWriterFailures() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AuditEventWriter failingWriter = event -> {
            latch.countDown();
            throw new RuntimeException("write failed");
        };

        AuditEventEmitter emitter = new AuditEventEmitter(failingWriter);
        try {
            emitter.emit(createEvent("evt-fail", AuditEventType.QUERY_FAILED));
            // Writer failure should not propagate
            assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            emitter.close();
        }
    }

    @Test
    void rejectsNullWriter() {
        assertThatThrownBy(() -> new AuditEventEmitter(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullEvent() {
        AuditEventEmitter emitter = new AuditEventEmitter(event -> {});
        try {
            assertThatThrownBy(() -> emitter.emit(null))
                    .isInstanceOf(NullPointerException.class);
        } finally {
            emitter.close();
        }
    }

    private AuditEvent createEvent(String eventId, AuditEventType type) {
        return new AuditEvent(eventId, type, Instant.now(), "alice", "127.0.0.1",
                "trace-1", "span-1", "q-1", "SELECT 1", "iceberg", "default",
                List.of(), true, null, 100, 1000, 2048, Map.of());
    }
}
