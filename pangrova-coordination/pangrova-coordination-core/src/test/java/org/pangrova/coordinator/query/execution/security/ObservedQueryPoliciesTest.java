package org.pangrova.coordinator.query.execution.security;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.security.api.AccessDeniedException;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservedQueryPoliciesTest {
    private static final QualifiedTableName TABLE = new QualifiedTableName("cat", "schema", "items");

    @Test
    void capturesExplicitAbsenceAndDefensivelyCopiesObservations() {
        ObservedQueryPolicies observations = new ObservedQueryPolicies();
        observations.rowFilter(TABLE, Optional.empty());
        observations.columnMask(TABLE, "id", Optional.empty());
        var snapshot = observations.snapshot();
        observations.columnMask(TABLE, "amount", Optional.of("0"));
        assertThat(snapshot.get(TABLE.toString()).columnMasks()).containsOnlyKeys("id");
    }

    @Test
    void rejectsChangedPolicyDuringOneCompilation() {
        ObservedQueryPolicies observations = new ObservedQueryPolicies();
        observations.rowFilter(TABLE, Optional.empty());
        assertThatThrownBy(() -> observations.rowFilter(TABLE, Optional.of("id > 0")))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("changed");
        observations.columnMask(TABLE, "id", Optional.empty());
        assertThatThrownBy(() -> observations.columnMask(TABLE, "id", Optional.of("0")))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void rejectsIncompletePolicyObservation() {
        ObservedQueryPolicies observations = new ObservedQueryPolicies();
        observations.rowFilter(TABLE, Optional.empty());
        assertThatThrownBy(observations::snapshot).isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("incomplete");
    }
}
