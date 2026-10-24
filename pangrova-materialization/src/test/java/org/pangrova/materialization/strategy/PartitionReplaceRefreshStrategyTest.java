package org.pangrova.materialization.strategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.materialization.MaterializationTestFixtures.ATTEMPT_ID;
import static org.pangrova.materialization.MaterializationTestFixtures.GENERATION_ID;
import static org.pangrova.materialization.MaterializationTestFixtures.INCARNATION;
import static org.pangrova.materialization.MaterializationTestFixtures.NOW;
import static org.pangrova.materialization.MaterializationTestFixtures.candidate;
import static org.pangrova.materialization.MaterializationTestFixtures.definition;
import static org.pangrova.materialization.MaterializationTestFixtures.evidence;
import static org.pangrova.materialization.MaterializationTestFixtures.objectId;
import static org.pangrova.materialization.MaterializationTestFixtures.source;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.PartitionAlignment;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshPlan;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.extension.RefreshContext;
import org.pangrova.spi.materialization.ChangeProof;
import org.pangrova.spi.materialization.ChangeSetMode;
import org.pangrova.spi.materialization.ChangeSetPlan;
import org.pangrova.spi.materialization.PartitionChange;
import org.pangrova.spi.materialization.PartitionValue;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;

class PartitionReplaceRefreshStrategyTest {

    private static final MaterializationId OBJECT = objectId("orders_dt");
    private static final SourceRef ORDERS = source("orders");
    private static final PhysicalTableRef BASE_TABLE = candidate(1);
    private static final PhysicalTableRef CANDIDATE = candidate(2);
    private static final String DEFINITION_SQL = "SELECT id, region, amount FROM lake.db.orders";
    private static final PartitionAlignment SCAN_ALIGNMENT = new PartitionAlignment(
            PartitionAlignment.Shape.SCAN,
            List.of(new PartitionAlignment.Column("region", "region", false)));

    private final PartitionReplaceRefreshStrategy strategy = new PartitionReplaceRefreshStrategy();

    @Test
    void plansAReplacementOfTheChangedPartitions() {
        RefreshContext context = context(SCAN_ALIGNMENT,
                changes(new PartitionChange(List.of(new PartitionValue("region", "'eu'")))));

        assertThat(strategy.validate(context).compatible()).isTrue();
        RefreshPlan plan = strategy.plan(context);

        assertThat(plan.kind()).isEqualTo(RefreshKind.IncrementalPartitionReplace.INSTANCE);
        assertThat(plan.canonicalSql()).isEqualTo(
                "SELECT * FROM " + RefreshSql.qualified(BASE_TABLE)
                        + " WHERE ((\"region\" IS NULL OR \"region\" <> 'eu'))"
                        + " UNION ALL SELECT * FROM (" + DEFINITION_SQL + ") AS pg_partition_refresh"
                        + " WHERE (\"region\" = 'eu')");
        assertThat(plan.changeSets()).containsOnlyKeys(ORDERS);
        assertThat(plan.planFingerprint()).hasSize(64);
    }

    @Test
    void plansSeveralPartitionsWithoutDroppingNullPartitions() {
        RefreshContext context = context(SCAN_ALIGNMENT,
                changes(
                        new PartitionChange(List.of(new PartitionValue("region", "'eu'"))),
                        new PartitionChange(List.of(new PartitionValue("region", null)))));

        String sql = strategy.plan(context).canonicalSql();

        assertThat(sql).contains(
                "WHERE ((\"region\" IS NULL OR \"region\" <> 'eu')) AND (\"region\" IS NOT NULL)");
        assertThat(sql).contains("WHERE (\"region\" = 'eu') OR (\"region\" IS NULL)");
    }

    @Test
    void anEmptyChangeSetCopiesTheBaseGeneration() {
        RefreshContext context = context(SCAN_ALIGNMENT, List.of());

        RefreshPlan plan = strategy.plan(context);

        assertThat(plan.canonicalSql())
                .isEqualTo("SELECT * FROM " + RefreshSql.qualified(BASE_TABLE));
        assertThat(plan.changeSets()).containsOnlyKeys(ORDERS);
    }

    @Test
    void requiresADirectlyProjectedPartitionColumn() {
        assertIncompatible(context(PartitionAlignment.NONE,
                changes(new PartitionChange(List.of(new PartitionValue("region", "'eu'"))))));
        assertIncompatible(context(new PartitionAlignment(PartitionAlignment.Shape.SCAN, List.of()),
                changes(new PartitionChange(List.of(new PartitionValue("region", "'eu'"))))));
    }

    @Test
    void requiresAPartitionColumnToBeAGroupingKeyInAggregateDefinitions() {
        PartitionAlignment notGrouped = new PartitionAlignment(
                PartitionAlignment.Shape.AGGREGATE,
                List.of(new PartitionAlignment.Column("region", "region", false)));

        assertIncompatible(context(notGrouped,
                changes(new PartitionChange(List.of(new PartitionValue("region", "'eu'"))))));
    }

    @Test
    void requiresAPartitionReplaceProofForThePlannedInterval() {
        assertIncompatible(context(SCAN_ALIGNMENT, List.of(),
                proof(ChangeSetMode.APPEND_ONLY, "snap-1", "snap-2", true)));
        assertIncompatible(context(SCAN_ALIGNMENT, List.of(),
                proof(ChangeSetMode.PARTITION_REPLACE, "snap-1", "snap-2", false)));
        assertIncompatible(context(SCAN_ALIGNMENT, List.of(),
                proof(ChangeSetMode.PARTITION_REPLACE, "snap-1", "snap-3", true)));
    }

    @Test
    void rejectsPartitionChangesWithInconsistentColumnSets() {
        assertIncompatible(context(SCAN_ALIGNMENT,
                changes(
                        new PartitionChange(List.of(new PartitionValue("region", "'eu'"))),
                        new PartitionChange(List.of(new PartitionValue("dt", "DATE '2024-01-01'"))))));
    }

    @Test
    void verifyRejectsProofsThatDoNotCoverThePlannedChangeSet() {
        RefreshContext context = context(SCAN_ALIGNMENT,
                changes(new PartitionChange(List.of(new PartitionValue("region", "'eu'")))));
        RefreshPlan plan = strategy.plan(context);

        strategy.verify(plan, proof(ChangeSetMode.PARTITION_REPLACE, "snap-1", "snap-2", true));
        assertThatThrownBy(() -> strategy.verify(plan,
                proof(ChangeSetMode.PARTITION_REPLACE, "snap-2", "snap-3", true)))
                .isInstanceOf(MaterializationException.class)
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED);
        assertThatThrownBy(() -> strategy.verify(plan,
                proof(ChangeSetMode.APPEND_ONLY, "snap-1", "snap-2", true)))
                .isInstanceOf(MaterializationException.class)
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INCREMENTAL_UNSUPPORTED);
    }

    private void assertIncompatible(RefreshContext context) {
        assertThat(strategy.validate(context).compatible()).isFalse();
        assertThatThrownBy(() -> strategy.plan(context))
                .isInstanceOf(MaterializationException.class)
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INCREMENTAL_UNSUPPORTED);
    }

    private static RefreshContext context(
            PartitionAlignment alignment, List<PartitionChange> changes) {
        return context(alignment, changes,
                proof(ChangeSetMode.PARTITION_REPLACE, "snap-1", "snap-2", true));
    }

    private static RefreshContext context(
            PartitionAlignment alignment, List<PartitionChange> changes, ChangeProof proof) {
        ChangeSetPlan changeSet = new ChangeSetPlan(ORDERS, snapshot("snap-1"), snapshot("snap-2"),
                ChangeSetMode.PARTITION_REPLACE, "change-set-1", NOW, changes);
        return new RefreshContext(definition(OBJECT, 1, DEFINITION_SQL),
                Optional.of(publishedBase()),
                new SourceVector(Map.of(ORDERS, snapshot("snap-2")), 2),
                CANDIDATE, Map.of(ORDERS, changeSet), Optional.of(proof), alignment);
    }

    private static List<PartitionChange> changes(PartitionChange... changes) {
        return List.of(changes);
    }

    private static Generation publishedBase() {
        return Generation.candidate(GENERATION_ID, OBJECT, INCARNATION, 1, ATTEMPT_ID,
                        Optional.empty(), BASE_TABLE, RefreshKind.Full.INSTANCE,
                        new SourceVector(Map.of(ORDERS, snapshot("snap-1")), 1), NOW)
                .commit(evidence("commit-1"), NOW)
                .publish(NOW);
    }

    private static SourceSnapshotRef snapshot(String token) {
        return new SourceSnapshotRef(ORDERS.connectorId(), token, "schema-v1", "policy-v1", NOW, true);
    }

    private static ChangeProof proof(ChangeSetMode mode, String fromToken, String toToken,
            boolean complete) {
        return new ChangeProof(ORDERS, fromToken, toToken, mode, "proof-1", "v1", NOW, complete);
    }
}
