package org.pangrova.coordinator.query.execution;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.session.QuerySession;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real parser and trusted-session selector validation; no execution or ref-resolution stand-ins. */
class TimeTravelExtractorTest {
    private static final QuerySession SESSION = new QuerySession(
            "query", Map.of("catalog", "Catalog", "schema", "Schema"), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

    @Test
    void resolvesOneTwoAndThreePartNamesFromTheTrustedSession() {
        var selected = TimeTravelExtractor.extract("SELECT id FROM first FOR VERSION AS OF 2 "
                + "UNION ALL SELECT id FROM other.second FOR SYSTEM_VERSION AS OF 3 "
                + "UNION ALL SELECT id FROM named.db.third FOR VERSION AS OF 4", SESSION, null);
        assertThat(selected.snapshotOverrides()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "catalog.schema.first", 2L, "catalog.other.second", 3L, "named.db.third", 4L));
        assertThat(selected.strippedSql()).doesNotContain("FOR VERSION", "FOR SYSTEM_VERSION");
    }

    @Test
    void rejectsMissingDefaultsAndNonpositiveOrNonliteralSnapshots() {
        var noDefaults = new QuerySession("query", Map.of(), null, "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        assertThatThrownBy(() -> TimeTravelExtractor.extract("SELECT id FROM rows FOR VERSION AS OF 2", noDefaults, null))
                .hasMessageContaining("default catalog");
        assertThatThrownBy(() -> TimeTravelExtractor.extract("SELECT id FROM rows FOR VERSION AS OF 0", SESSION, null))
                .hasMessageContaining("positive");
        assertThatThrownBy(() -> TimeTravelExtractor.extract("SELECT id FROM rows FOR VERSION AS OF id", SESSION, null))
                .hasMessageContaining("integer snapshot");
    }

    @Test
    void rejectsConflictingVersionsAndCurrentHistoricalAliasesInsteadOfLastWins() {
        assertThatThrownBy(() -> TimeTravelExtractor.extract("SELECT a.id FROM rows FOR VERSION AS OF 2 a "
                + "JOIN rows FOR VERSION AS OF 3 b ON a.id = b.id", SESSION, null))
                .hasMessageContaining("per-relation read bindings");
        assertThatThrownBy(() -> TimeTravelExtractor.extract("SELECT a.id FROM rows FOR VERSION AS OF 2 a "
                + "JOIN Catalog.Schema.rows b ON a.id = b.id", SESSION, null))
                .hasMessageContaining("per-relation read bindings");
        assertThat(TimeTravelExtractor.extract("SELECT a.id FROM rows FOR VERSION AS OF 2 a "
                + "JOIN Catalog.Schema.rows FOR VERSION AS OF 2 b ON a.id = b.id", SESSION, null)
                .snapshotOverrides()).containsOnlyKeys("catalog.schema.rows");
    }

    @Test
    void logicalCteIsNotQualifiedAsAPhysicalTableAndScalarSourcesAreCollected() {
        var result = TimeTravelExtractor.extract("WITH rows AS (SELECT id FROM source FOR VERSION AS OF 2) "
                + "SELECT (SELECT id FROM other FOR VERSION AS OF 3) FROM rows", SESSION, null);
        assertThat(result.snapshotOverrides()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "catalog.schema.source", 2L, "catalog.schema.other", 3L));
        assertThatThrownBy(() -> TimeTravelExtractor.extract(
                "WITH rows AS (SELECT id FROM source) SELECT id FROM rows FOR VERSION AS OF 2", SESSION, null))
                .hasMessageContaining("logical CTE");
    }

    @Test
    void quotedLiteralDotsAreNotSplitOrCollidedWithQualification() {
        var result = TimeTravelExtractor.extract("SELECT id FROM Catalog.\"a.b\".rows FOR VERSION AS OF 2 "
                + "UNION ALL SELECT id FROM Catalog.a.\"b.rows\" FOR VERSION AS OF 3", SESSION, null);
        assertThat(result.snapshotOverrides()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "catalog.\"a.b\".rows", 2L, "catalog.a.\"b.rows\"", 3L));
    }

    @Test
    void branchWithoutARealResolverAndMalformedSqlNeverBecomeCurrentReads() {
        assertThatThrownBy(() -> TimeTravelExtractor.extract(
                "SELECT id FROM rows VERSION AS OF BRANCH 'missing'", SESSION, null)).hasMessageContaining("ref resolver");
        assertThatThrownBy(() -> TimeTravelExtractor.extract(
                "SELECT id FROM rows FOR VERSION AS OF", SESSION, null)).isInstanceOf(org.pangrova.parser.api.exception.ParseException.class);
    }
}
