package org.pangrova.governance.classification;

import org.pangrova.governance.lineage.LineageCatalog;
import org.pangrova.governance.lineage.LineageRecord;
import org.pangrova.governance.lineage.LineageRecord.ColumnDerivationEntry;
import org.pangrova.governance.lineage.LineageRecord.SourceColumnRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests lineage-driven tag propagation: tagging a source column automatically
 * propagates that tag to all derived output columns recorded in the lineage catalog.
 *
 * <p>Validates the G-SEC-04 fix for orphaned tag-store routing: tags applied to a
 * source column must trigger masking on derived objects produced by CTAS/INSERT...SELECT.
 */
class LineageTagPropagatorTest {
    private final java.util.Map<org.pangrova.spi.connector.metadata.table.QualifiedTableName, java.util.UUID>
            tableIds = new java.util.HashMap<>();


    private ColumnTagService tagService;
    private LineageCatalog lineageCatalog;
    private LineageTagPropagator propagator;

    @BeforeEach
    void setUp() {
        tagService = new ColumnTagService(table -> tableIds.computeIfAbsent(table, ignored -> java.util.UUID.randomUUID()));
        lineageCatalog = new LineageCatalog();
        propagator = new LineageTagPropagator(tagService, lineageCatalog);
        // Register the propagator as a listener on the tag service
        tagService.addListener(propagator);
    }

    @Test
    void tagOnSourceColumnPropagaTesToDerivedColumn() {
        // Set up lineage: source.users.email -> derived.report.user_email
        lineageCatalog.store(new LineageRecord(
                "q1",
                "INSERT INTO cat.analytics.report SELECT email AS user_email FROM cat.raw.users",
                List.of(new ColumnDerivationEntry(
                        "user_email",
                        List.of(new SourceColumnRef("cat.raw.users", "email")),
                        "DIRECT")),
                Set.of("cat.raw.users"),
                Set.of("cat.analytics.report"),
                System.currentTimeMillis()));

        // Tag the source column
        tagService.assignTag("cat", "raw", "users", "email",
                "pii_type", null, "PII_EMAIL");

        // Verify the tag propagated to the derived column
        Map<String, String> derivedTags = tagService.getTags(
                "cat", "analytics", "report", "user_email");
        assertThat(derivedTags).containsEntry("pii_type", "PII_EMAIL");
    }

    @Test
    void tagPropagationSkipsDuplicates() {
        // Pre-tag the derived column with the same value
        tagService.assignTag("cat", "analytics", "report", "user_email",
                "pii_type", null, "PII_EMAIL");

        lineageCatalog.store(new LineageRecord(
                "q2",
                "INSERT INTO cat.analytics.report SELECT email AS user_email FROM cat.raw.users",
                List.of(new ColumnDerivationEntry(
                        "user_email",
                        List.of(new SourceColumnRef("cat.raw.users", "email")),
                        "DIRECT")),
                Set.of("cat.raw.users"),
                Set.of("cat.analytics.report"),
                System.currentTimeMillis()));

        // Tag with the same value should not re-assign
        int propagated = propagator.propagateToDownstream(
                "cat.raw.users", "email", "pii_type", "PII_EMAIL");
        assertThat(propagated).isZero();
    }

    @Test
    void tagRemovalDoesNotPropagateToDownstream() {
        // Set up lineage
        lineageCatalog.store(new LineageRecord(
                "q3",
                "CREATE TABLE cat.analytics.derived AS SELECT ssn FROM cat.raw.users",
                List.of(new ColumnDerivationEntry(
                        "ssn",
                        List.of(new SourceColumnRef("cat.raw.users", "ssn")),
                        "DIRECT")),
                Set.of("cat.raw.users"),
                Set.of("cat.analytics.derived"),
                System.currentTimeMillis()));

        // Pre-tag both source and derived
        tagService.assignTag("cat", "raw", "users", "ssn", "pii_type", null, "PII_SSN");
        tagService.assignTag("cat", "analytics", "derived", "ssn", "pii_type", null, "PII_SSN");

        // Remove tag from source — should NOT propagate removal to derived
        tagService.removeTag("cat", "raw", "users", "ssn", "pii_type", "PII_SSN");

        // Derived column should retain its tag
        Map<String, String> derivedTags = tagService.getTags(
                "cat", "analytics", "derived", "ssn");
        assertThat(derivedTags).containsEntry("pii_type", "PII_SSN");
    }

    @Test
    void tableLevelTagDoesNotTriggerPropagation() {
        // Set up lineage
        lineageCatalog.store(new LineageRecord(
                "q4",
                "INSERT INTO cat.analytics.derived SELECT id FROM cat.raw.users",
                List.of(new ColumnDerivationEntry(
                        "id",
                        List.of(new SourceColumnRef("cat.raw.users", "id")),
                        "DIRECT")),
                Set.of("cat.raw.users"),
                Set.of("cat.analytics.derived"),
                System.currentTimeMillis()));

        // Table-level tag event (columnName == null)
        TagChangedEvent tableEvent = new TagChangedEvent(
                "cat", "raw", "users", null,
                "sensitivity_level", null, "RESTRICTED", null);
        propagator.onTagChanged(tableEvent);

        // No column tags should have been added to derived
        Map<String, String> derivedTags = tagService.getTags(
                "cat", "analytics", "derived", "id");
        assertThat(derivedTags).isEmpty();
    }

    @Test
    void multipleDerivationsFromSameSourceAllReceiveTag() {
        // Source column feeds two different target tables
        lineageCatalog.store(new LineageRecord(
                "q5a",
                "INSERT INTO cat.report.summary SELECT name FROM cat.raw.customers",
                List.of(new ColumnDerivationEntry(
                        "customer_name",
                        List.of(new SourceColumnRef("cat.raw.customers", "name")),
                        "DIRECT")),
                Set.of("cat.raw.customers"),
                Set.of("cat.report.summary"),
                System.currentTimeMillis()));

        lineageCatalog.store(new LineageRecord(
                "q5b",
                "INSERT INTO cat.ml.features SELECT name AS feature_name FROM cat.raw.customers",
                List.of(new ColumnDerivationEntry(
                        "feature_name",
                        List.of(new SourceColumnRef("cat.raw.customers", "name")),
                        "DIRECT")),
                Set.of("cat.raw.customers"),
                Set.of("cat.ml.features"),
                System.currentTimeMillis()));

        // Tag the source
        tagService.assignTag("cat", "raw", "customers", "name",
                "pii_type", null, "PII_NAME");

        // Both derived columns should receive the tag
        assertThat(tagService.getTags("cat", "report", "summary", "customer_name"))
                .containsEntry("pii_type", "PII_NAME");
        assertThat(tagService.getTags("cat", "ml", "features", "feature_name"))
                .containsEntry("pii_type", "PII_NAME");
    }

    @Test
    void splitQualifiedNameHandlesEdgeCases() {
        assertThat(LineageTagPropagator.splitQualifiedName("a.b.c"))
                .containsExactly("a", "b", "c");
        assertThat(LineageTagPropagator.splitQualifiedName("only_two.parts"))
                .isNull();
        assertThat(LineageTagPropagator.splitQualifiedName("a.b.c.d"))
                .isNotNull(); // split(3) keeps remainder in third part
    }
}
