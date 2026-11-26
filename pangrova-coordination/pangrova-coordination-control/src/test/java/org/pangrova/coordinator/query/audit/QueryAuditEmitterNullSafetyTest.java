package org.pangrova.coordinator.query.audit;

import org.pangrova.query.audit.QueryAuditEmitter;
import org.pangrova.query.api.audit.QueryAuditRecord;
import org.pangrova.query.api.enums.QueryState;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThat;

import org.pangrova.query.api.session.QuerySession;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

/**
 * Verifies QueryAuditEmitter null safety: no NPE when both audit backends are absent.
 */
class QueryAuditEmitterNullSafetyTest {

    @Test
    void noOpEmitterDoesNotThrowOnEmit() {
        QueryAuditEmitter emitter = QueryAuditEmitter.noOp();
        QuerySession session = new QuerySession("q-1", Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

        assertThatCode(() -> emitter.emitQuerySubmitted("q-1", "SELECT 1", session))
                .doesNotThrowAnyException();
        assertThatCode(() -> emitter.emitQueryTerminal(QueryState.FINISHED,
                QueryAuditRecord.wallTimeOnly("q-1", "anonymous", null, null, 100, Instant.now(), true), null))
                .doesNotThrowAnyException();
        assertThatCode(() -> emitter.emitQueryTerminal(QueryState.FAILED,
                QueryAuditRecord.wallTimeOnly("q-1", "anonymous", null, null, 50, Instant.now(), false), "error"))
                .doesNotThrowAnyException();
        assertThatCode(() -> emitter.emitTableAccessed("q-1", "cat", "sch", "tbl", session))
                .doesNotThrowAnyException();
    }

    @Test
    void nullConstructorArgsProducesSafeEmitter() {
        QueryAuditEmitter emitter = new QueryAuditEmitter(null, null);

        assertThatCode(() -> emitter.emitQuerySubmitted("q-2", "SELECT 2", null))
                .doesNotThrowAnyException();
        assertThatCode(() -> emitter.emitQueryTerminal(QueryState.FINISHED,
                QueryAuditRecord.wallTimeOnly("q-2", "anonymous", null, null, 200, Instant.now(), true), null))
                .doesNotThrowAnyException();
    }

    @Test
    void noOpFactoryReturnsNonNullInstance() {
        assertThat(QueryAuditEmitter.noOp()).isNotNull();
    }
}
