package org.pangrova.coordinator.feature.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.feature.FeatureViewDefinitionValidator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Logical definition persistence must be deterministic and never create an executable-plan input. */
class FeatureDefinitionCanonicalizationPropertyTest {
    private final FeatureViewDefinitionValidator validator = new FeatureViewDefinitionValidator();

    @Property void canonicalDefinitionCanBeRevalidatedWithoutChangingTypedContract(@ForAll("identifiers") String table) {
        var first = validator.validate(definition(table));
        var persisted = validator.validate(first.canonicalJson());
        assertThat(persisted).isEqualTo(first);
        assertThat(first.canonicalJson()).doesNotContain("source_plan");
    }

    @Property void clientExecutionPlanIsRejectedIndependentOfItsPayload(@ForAll("identifiers") String payload) {
        String supplied = definition("rows").replace("\"source_query\":", "\"source_plan_base64\":\""
                + payload + "\",\"source_query\":");
        assertThatThrownBy(() -> validator.validate(supplied)).hasMessageContaining("source_plan_base64");
    }

    @Provide Arbitrary<String> identifiers() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
    }

    private static String definition(String table) {
        return "{\"entity_keys\":[\"id\"],\"event_time\":\"ts\",\"source_table\":\"lake.raw." + table
                + "\",\"source_query\":\"SELECT id,ts FROM " + table + "\",\"transform_version\":\"v1\","
                + "\"schema\":{\"ts\":\"TIMESTAMP\",\"id\":\"BIGINT\"},\"freshness_target_ms\":1,"
                + "\"lineage\":[\"lake.raw." + table + "\"]}";
    }
}
