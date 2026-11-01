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

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshPlan;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.extension.RefreshContext;
import org.pangrova.materialization.extension.RefreshStrategy;
import org.pangrova.spi.connector.materialization.ChangeProof;
import org.pangrova.spi.connector.materialization.ChangeSetMode;
import org.pangrova.spi.connector.materialization.ChangeSetPlan;
import org.pangrova.spi.connector.materialization.SourceRef;
import org.pangrova.spi.connector.materialization.SourceSnapshotRef;

class RefreshStrategyTest {

    private static final MaterializationId OBJECT = objectId("orders_dt");
    private static final SourceRef ORDERS = source("orders");
    private static final PhysicalTableRef BASE_TABLE = candidate(1);
    private static final PhysicalTableRef CANDIDATE = candidate(2);
    private static final String DEFINITION_SQL = "SELECT id, name FROM lake.db.orders";

    private final RefreshStrategy full = new FullRefreshStrategy();
    private final RefreshStrategy appendOnly = new AppendOnlyRefreshStrategy();

    @Test
    void fullRefreshPlansTheDefinitionAndCarriesNoChangeSet() {
        RefreshContext context = fullContext();

        assertThat(full.validate(context).compatible()).isTrue();
        RefreshPlan plan = full.plan(context);

        assertThat(plan.kind()).isEqualTo(RefreshKind.Full.INSTANCE);
        assertThat(plan.canonicalSql()).isEqualTo(DEFINITION_SQL);
        assertThat(plan.changeSets()).isEmpty();
        assertThat(plan.planFingerprint()).hasSize(64);
        assertThat(plan.planFingerprint()).isEqualTo(full.plan(context).planFingerprint());
    }

    @Test
    void fullRefreshStripsTrailingStatementTerminators() {
        RefreshContext context = new RefreshContext(
                definition(OBJECT, 1, DEFINITION_SQL + " ; ;"), Optional.empty(),
                targetVector("snap-1"), CANDIDATE);

        assertThat(full.plan(context).canonicalSql()).isEqualTo(DEFINITION_SQL);
    }

    @Test
    void fullRefreshRejectsBaseGenerationsAndChangeSets() {
        RefreshContext withBase = new RefreshContext(
                definition(OBJECT, 1, DEFINITION_SQL),
                Optional.of(publishedBase(baseVector("snap-1"), 1)),
                targetVector("snap-2"), CANDIDATE);

        assertThat(full.validate(withBase).compatible()).isFalse();
        assertThatThrownBy(() -> full.plan(withBase))
                .isInstanceOf(MaterializationException.class)
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INCREMENTAL_UNSUPPORTED);
        assertThatThrownBy(() -> full.verify(full.plan(fullContext()), proof("snap-1", "snap-2", true)))
                .isInstanceOf(MaterializationException.class)
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED);
    }

    @Test
    void appendOnlyPlansBaseUnionAllDelta() {
        RefreshContext context = appendOnlyContext(1, "snap-1", "snap-2", proof("snap-1", "snap-2", true));

        assertThat(appendOnly.validate(context).compatible()).isTrue();
        RefreshPlan plan = appendOnly.plan(context);

        assertThat(plan.kind()).isEqualTo(RefreshKind.IncrementalAppendOnly.INSTANCE);
        assertThat(plan.canonicalSql()).isEqualTo("SELECT * FROM \"lake\"."
                + '"' + BASE_TABLE.schema() + "\".\"" + BASE_TABLE.table() + "\" UNION ALL " + DEFINITION_SQL);
        assertThat(plan.changeSets()).containsOnlyKeys(ORDERS);
        assertThat(plan.changeSets().get(ORDERS).planFingerprint()).isEqualTo("change-set-1");
        assertThat(plan.planFingerprint()).hasSize(64);
    }

    @Test
    void appendOnlyRejectsAMissingPublishedBaseAtTheCurrentRevision() {
        assertIncompatible(new RefreshContext(definition(OBJECT, 1, DEFINITION_SQL), Optional.empty(),
                targetVector("snap-2"), CANDIDATE, changeSets("snap-1", "snap-2"),
                Optional.of(proof("snap-1", "snap-2", true))));
        assertIncompatible(new RefreshContext(definition(OBJECT, 2, DEFINITION_SQL),
                Optional.of(publishedBase(baseVector("snap-1"), 1)),
                targetVector("snap-2"), CANDIDATE, changeSets("snap-1", "snap-2"),
                Optional.of(proof("snap-1", "snap-2", true))));
    }

    @Test
    void appendOnlyRequiresExactlyOnePlannedSourceMatchingTheBase() {
        SourceRef customers = source("customers");
        DefinitionRevision twoSources = definition(OBJECT, 1, DEFINITION_SQL, Set.of(ORDERS, customers));
        SourceVector twoSourceVector = new SourceVector(
                Map.of(ORDERS, snapshot(ORDERS, "snap-2"), customers, snapshot(customers, "snap-9")), 2);
        assertIncompatible(new RefreshContext(twoSources,
                Optional.of(publishedBase(baseVector("snap-1"), 1)),
                twoSourceVector, CANDIDATE, changeSets("snap-1", "snap-2"),
                Optional.of(proof("snap-1", "snap-2", true))));

        SourceRef other = source("other_orders");
        DefinitionRevision otherSource = definition(OBJECT, 1, DEFINITION_SQL, Set.of(other));
        assertIncompatible(new RefreshContext(otherSource,
                Optional.of(publishedBase(baseVector("snap-1"), 1)),
                new SourceVector(Map.of(other, snapshot(other, "snap-2")), 2), CANDIDATE,
                changeSets("snap-1", "snap-2"), Optional.of(proof("snap-1", "snap-2", true))));
    }

    @Test
    void appendOnlyRequiresACompleteAppendOnlyProofForThePlannedInterval() {
        assertIncompatible(appendOnlyContext(1, "snap-1", "snap-2", null));
        assertIncompatible(appendOnlyContext(1, "snap-1", "snap-2", proof("snap-1", "snap-2", false)));
        assertIncompatible(appendOnlyContext(1, "snap-1", "snap-2",
                new ChangeProof(ORDERS, "snap-1", "snap-2", ChangeSetMode.PARTITION_REPLACE,
                        "proof-1", "v1", NOW, true)));
        assertIncompatible(appendOnlyContext(1, "snap-1", "snap-2", proof("snap-0", "snap-2", true)));
        assertIncompatible(appendOnlyContext(1, "snap-1", "snap-2", proof("snap-1", "snap-3", true)));
        assertIncompatible(appendOnlyContext(1, "snap-1", "snap-2", proof("snap-1", "snap-1", true)));
    }

    @Test
    void appendOnlyValidateChecksTheChangeProofAgainstThePlannedChangeSet() {
        RefreshContext context = new RefreshContext(def(), Optional.of(publishedBase(baseVector("snap-1"), 1)),
                targetVector("snap-2"), CANDIDATE, changeSets("snap-1", "snap-3"),
                Optional.of(proof("snap-1", "snap-3", true)));

        assertIncompatible(context);
    }

    @Test
    void appendOnlyVerifyRejectsProofsThatDoNotCoverThePlannedChangeSet() {
        RefreshContext context = appendOnlyContext(1, "snap-1", "snap-2", proof("snap-1", "snap-2", true));
        RefreshPlan plan = appendOnly.plan(context);

        appendOnly.verify(plan, proof("snap-1", "snap-2", true));
        assertThatThrownBy(() -> appendOnly.verify(plan, proof("snap-2", "snap-3", true)))
                .isInstanceOf(MaterializationException.class)
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED);
        assertThatThrownBy(() -> appendOnly.verify(plan, proof("snap-1", "snap-2", false)))
                .isInstanceOf(MaterializationException.class)
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INCREMENTAL_UNSUPPORTED);
        assertThatThrownBy(() -> appendOnly.verify(full.plan(fullContext()), proof("snap-1", "snap-2", true)))
                .isInstanceOf(MaterializationException.class)
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_REQUEST_REJECTED);
    }

    @Test
    void refreshContextAcceptsARepublishedGeneratedUpstream() {
        SourceRef upstreamBefore = new SourceRef(
                "iceberg", "lake", "__pangrova_materializations", "__pg_mat_before");
        SourceRef upstreamAfter = new SourceRef(
                "iceberg", "lake", "__pangrova_materializations", "__pg_mat_after");
        DefinitionRevision definition = definition(OBJECT, 1, DEFINITION_SQL,
                Set.of(ORDERS, upstreamBefore));
        SourceVector vector = new SourceVector(Map.of(
                ORDERS, snapshot(ORDERS, "snap-1"),
                upstreamAfter, snapshot(upstreamAfter, "snap-1")), 2);

        RefreshContext context = new RefreshContext(definition, Optional.empty(), vector, CANDIDATE);

        assertThat(full.validate(context).compatible()).isTrue();
        assertThatThrownBy(() -> new RefreshContext(definition, Optional.empty(),
                new SourceVector(Map.of(
                        source("customers"), snapshot(source("customers"), "snap-1"),
                        upstreamAfter, snapshot(upstreamAfter, "snap-1")), 2), CANDIDATE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("definition sources");
    }

    private void assertIncompatible(RefreshContext context) {
        assertThat(appendOnly.validate(context).compatible()).isFalse();
        assertThatThrownBy(() -> appendOnly.plan(context))
                .isInstanceOf(MaterializationException.class)
                .extracting(failure -> ((MaterializationException) failure).getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_INCREMENTAL_UNSUPPORTED);
    }

    private static RefreshContext fullContext() {
        return new RefreshContext(def(), Optional.empty(), targetVector("snap-1"), CANDIDATE);
    }

    private static RefreshContext appendOnlyContext(
            long revision, String fromToken, String toToken, ChangeProof proof) {
        return new RefreshContext(definition(OBJECT, revision, DEFINITION_SQL),
                Optional.of(publishedBase(baseVector(fromToken), revision)),
                targetVector(toToken), CANDIDATE,
                proof == null ? Map.of() : changeSets(fromToken, toToken),
                Optional.ofNullable(proof));
    }

    private static DefinitionRevision def() {
        return definition(OBJECT, 1, DEFINITION_SQL);
    }

    private static Generation publishedBase(SourceVector vector, long revision) {
        return Generation.candidate(GENERATION_ID, OBJECT, INCARNATION, revision, ATTEMPT_ID,
                        Optional.empty(), BASE_TABLE, RefreshKind.Full.INSTANCE, vector, NOW)
                .commit(evidence("commit-1"), NOW)
                .publish(NOW);
    }

    private static SourceVector baseVector(String token) {
        return new SourceVector(Map.of(ORDERS, snapshot(ORDERS, token)), 1);
    }

    private static SourceVector targetVector(String token) {
        return new SourceVector(Map.of(ORDERS, snapshot(ORDERS, token)), 2);
    }

    private static SourceSnapshotRef snapshot(SourceRef source, String token) {
        return new SourceSnapshotRef(source.connectorId(), token, "schema-v1", "policy-v1", NOW, true);
    }

    private static Map<SourceRef, ChangeSetPlan> changeSets(String fromToken, String toToken) {
        return Map.of(ORDERS, new ChangeSetPlan(ORDERS, snapshot(ORDERS, fromToken),
                snapshot(ORDERS, toToken), ChangeSetMode.APPEND_ONLY, "change-set-1", NOW,
                java.util.List.of()));
    }

    private static ChangeProof proof(String fromToken, String toToken, boolean complete) {
        return new ChangeProof(ORDERS, fromToken, toToken, ChangeSetMode.APPEND_ONLY,
                "proof-1", "v1", NOW, complete);
    }
}
