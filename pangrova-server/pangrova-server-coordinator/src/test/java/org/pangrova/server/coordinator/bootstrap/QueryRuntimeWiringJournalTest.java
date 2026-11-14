package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryRuntimeWiringJournalTest {

    @Test
    void asyncDispatchCapturesRequestLoggingFieldsAtSubmission() throws Exception {
        var executor = QueryRuntimeWiring.createAsyncSubmitExecutor();
        try {
            var first = executorRequest(executor, "first");
            var second = executorRequest(executor, "second");
            assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo("first");
            assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo("second");
            assertThat(executor.submit(() -> MDC.get("requestId")).get(5, TimeUnit.SECONDS)).isNull();
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static java.util.concurrent.Future<String> executorRequest(
            java.util.concurrent.ExecutorService executor, String requestId) {
        try (var ignored = MDC.putCloseable("requestId", requestId)) {
            return executor.submit(() -> MDC.get("requestId"));
        }
    }

    @Test
    void missingJournalIsRejectedBeforeConstructingRuntimeDependencies() {
        // No other dependencies are available yet. Journal validation must happen
        // before accessing them or acquiring the owned async executor.
        var wiring = new QueryRuntimeWiring(new QueryRuntimeWiring.Dependencies(
                null, null, null, null, null, null));

        assertThatThrownBy(() -> wiring.buildQueryManager(null, null, null, Optional.empty()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("stateJournal must be selected explicitly");
    }
}
