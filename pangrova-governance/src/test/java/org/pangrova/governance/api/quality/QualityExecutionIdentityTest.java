package org.pangrova.governance.api.quality;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QualityExecutionIdentityTest {
    @Test
    void neitherAnEmptyPrincipalNorAnEmptyTenantIsAnExecutionSubject() {
        assertThatThrownBy(() -> new QualityExecutionIdentity(null, "tenant"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QualityExecutionIdentity("owner", " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void enabledRuleCannotInventAnOwnerForALegacyDefinition() {
        assertThatThrownBy(() -> new DataQualityRule(1, "rule", "catalog", "schema", "table",
                "id > 0", Severity.ERROR, null, true, null, Instant.now(), Instant.now(), null, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("execution identity");
    }
}
