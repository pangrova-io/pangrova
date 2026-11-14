package org.pangrova.coordinator.query;

import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.constants.TestObservability;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryManagerContextJournalTest {

    @Test
    void missingJournalCannotSelectAnImplicitMemoryBackend() {
        assertThatThrownBy(() -> new QueryManagerContext(
                null, TestObservability.TRACING,
                null, null, null, null, null, null, null, null, null, new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("stateJournal must be selected explicitly");
    }
}
