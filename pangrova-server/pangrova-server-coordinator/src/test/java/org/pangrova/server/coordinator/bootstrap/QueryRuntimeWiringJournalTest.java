package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryRuntimeWiringJournalTest {

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
