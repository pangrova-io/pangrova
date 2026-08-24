package org.pangrova.spi.resource.classification;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the workload classification SPI contracts (Task 146, G-RES-05).
 */
class WorkloadClassifierSpiTest {

    // --- QueryAttributes ---

    @Test
    void queryAttributesStoresFields() {
        var attrs = new QueryAttributes(
                Optional.of("alice"), Optional.of("jdbc"),
                Set.of("adhoc", "dev"), Optional.of("SELECT"),
                Optional.of("analytics"), Optional.of("10.0.0.1"));
        assertThat(attrs.user()).hasValue("alice");
        assertThat(attrs.source()).hasValue("jdbc");
        assertThat(attrs.clientTags()).containsExactlyInAnyOrder("adhoc", "dev");
        assertThat(attrs.queryType()).hasValue("SELECT");
        assertThat(attrs.database()).hasValue("analytics");
        assertThat(attrs.sourceIp()).hasValue("10.0.0.1");
    }

    @Test
    void queryAttributesDefaultsNullsToEmpty() {
        var attrs = new QueryAttributes(null, null, null, null, null, null);
        assertThat(attrs.user()).isEmpty();
        assertThat(attrs.source()).isEmpty();
        assertThat(attrs.clientTags()).isEmpty();
        assertThat(attrs.queryType()).isEmpty();
        assertThat(attrs.database()).isEmpty();
        assertThat(attrs.sourceIp()).isEmpty();
    }

    @Test
    void queryAttributesForUserFactory() {
        var attrs = QueryAttributes.forUser("bob");
        assertThat(attrs.user()).hasValue("bob");
        assertThat(attrs.clientTags()).isEmpty();
    }

    @Test
    void queryAttributesClientTagsAreImmutable() {
        var original = new java.util.HashSet<>(Set.of("a", "b"));
        var attrs = new QueryAttributes(
                Optional.empty(), Optional.empty(), original,
                Optional.empty(), Optional.empty(), Optional.empty());
        original.add("c");
        assertThat(attrs.clientTags()).doesNotContain("c");
    }

    // --- WorkloadClass ---

    @Test
    void workloadClassStoresFields() {
        var wc = new WorkloadClass("adhoc-group", 5);
        assertThat(wc.resourceGroupId()).isEqualTo("adhoc-group");
        assertThat(wc.priority()).isEqualTo(5);
    }

    @Test
    void workloadClassDefaultPriority() {
        var wc = WorkloadClass.of("default");
        assertThat(wc.resourceGroupId()).isEqualTo("default");
        assertThat(wc.priority()).isEqualTo(WorkloadClass.DEFAULT_PRIORITY);
    }

    @Test
    void workloadClassRejectsBlankGroup() {
        assertThatThrownBy(() -> new WorkloadClass(" ", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resourceGroupId");
    }

    @Test
    void workloadClassRejectsNullGroup() {
        assertThatThrownBy(() -> new WorkloadClass(null, 0))
                .isInstanceOf(NullPointerException.class);
    }

    // --- WorkloadClassifier interface ---

    @Test
    void classifierInterfaceCanBeImplemented() {
        WorkloadClassifier classifier = new WorkloadClassifier() {
            @Override
            public WorkloadClass classify(QueryAttributes attributes) {
                // admin users go to priority group
                if (attributes.user().map("admin"::equals).orElse(false)) {
                    return new WorkloadClass("admin-group", 10);
                }
                // INSERT queries go to etl group
                if (attributes.queryType().map("INSERT"::equalsIgnoreCase).orElse(false)) {
                    return new WorkloadClass("etl-group", 3);
                }
                return WorkloadClass.of("default");
            }

            @Override
            public String name() {
                return "rule-based";
            }
        };

        // admin user
        var adminAttrs = QueryAttributes.forUser("admin");
        assertThat(classifier.classify(adminAttrs).resourceGroupId()).isEqualTo("admin-group");
        assertThat(classifier.classify(adminAttrs).priority()).isEqualTo(10);

        // insert query
        var insertAttrs = new QueryAttributes(
                Optional.of("alice"), Optional.empty(), Set.of(),
                Optional.of("INSERT"), Optional.empty(), Optional.empty());
        assertThat(classifier.classify(insertAttrs).resourceGroupId()).isEqualTo("etl-group");

        // default
        var selectAttrs = QueryAttributes.forUser("bob");
        assertThat(classifier.classify(selectAttrs).resourceGroupId()).isEqualTo("default");

        assertThat(classifier.name()).isEqualTo("rule-based");
    }
}
