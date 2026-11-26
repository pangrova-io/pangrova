package org.pangrova.persistence.indexing.postgresql.metadata;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.index.SnapshotPinOwner;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.indexing.api.build.ScalarArtifactIntent;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.postgresql.build.JdbcIndexBuildClaimRepository;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcScalarArtifactIntentStoreTest extends ContainerTestBase {
    private static final Instant START = Instant.parse("2026-11-01T00:00:00Z");
    private static final Instant FUTURE = Instant.parse("2099-01-01T00:00:00Z");

    @Test
    void durableIntentSurvivesLostOwnerAndMappingRollback() throws Exception {
        var source = SharedPostgresContainer.migratedDataSource("scalar_artifact_intents");
        verifyLifecycle(source);
        verifyIndependentPurgeFences(source);
    }

    static void verifyIndependentPurgeFences(DataSource source) {
        var store = new JdbcIndexMetadataStore(source, TableWriteAuthorityFixture.create()::requireCurrent);
        var claims = new JdbcIndexBuildClaimRepository(source);
        String indexId = "independent-purge-fences";
        String snapshot = "snapshot-0";
        var descriptor = new IndexDescriptor(indexId, "independent", "lake", "raw", "events", List.of("value"),
                IndexType.HASH, Map.of(), IndexState.BUILDING, null, 0, snapshot, null,
                START, START, "tenant");
        store.registerIndexWithBuildAuthority(descriptor, "builder");
        String ownerId = UUID.randomUUID().toString();
        var owner = new SnapshotPinOwner(indexId, 0, ownerId);
        var artifact = new ScalarArtifactIntent(indexId, 0, ownerId, "source", "/fixture/independent-purge.idx");
        assertThat(claims.tryClaim(indexId, 0, ownerId, START)).isTrue();
        assertThat(store.reserveScalarSnapshotPin(descriptor, snapshot, owner, START)).isTrue();
        store.beginScalarArtifactWrite(artifact);
        assertThat(claims.release(indexId, 0, ownerId)).isTrue();
        store.dropIndex(indexId, START.plusSeconds(1));
        assertThatThrownBy(() -> store.purgeIndex(indexId)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("snapshot pin cleanup");

        store.markScalarSnapshotPinDrained(owner, snapshot, START.plusSeconds(2));
        assertThat(store.claimScalarSnapshotPinCleanup(owner, START.plusSeconds(3))).isPresent();
        store.markScalarSnapshotPinReleased(owner, snapshot, START.plusSeconds(4));
        assertThatThrownBy(() -> store.purgeIndex(indexId)).isInstanceOf(MetastoreException.class);
        assertThat(store.findIndex(indexId)).isPresent();

        store.markScalarArtifactDrained(artifact);
        assertThat(store.beginScalarArtifactReclaim(artifact)).isTrue();
        // This is a database-only fixture: no external artifact was written.
        store.completeScalarArtifactReclaim(artifact);
        store.purgeIndex(indexId);
        assertThat(store.findIndex(indexId)).isEmpty();
    }

    static void verifyLifecycle(DataSource source) {
        var store = new JdbcIndexMetadataStore(source, TableWriteAuthorityFixture.create()::requireCurrent);
        var claims = new JdbcIndexBuildClaimRepository(source);
        store.registerIndex(new IndexDescriptor("index", "idx", "lake", "raw", "events", List.of("value"),
                IndexType.HASH, Map.of(), IndexState.BUILDING, null, 0, "snapshot-0", null,
                START, START, "tenant"));
        String oldOwner = UUID.randomUUID().toString();
        String newOwner = UUID.randomUUID().toString();
        var uncertain = intent(0, oldOwner, "uncertain");
        var late = intent(0, oldOwner, "late");
        assertThatThrownBy(() -> store.beginScalarArtifactWrite(late)).isInstanceOf(IllegalStateException.class);
        assertThat(claims.tryClaim("index", 0, oldOwner, START)).isTrue();
        store.beginScalarArtifactWrite(uncertain);
        store.beginScalarArtifactWrite(late);
        assertThat(store.listReclaimableScalarArtifacts(FUTURE, 10)).isEmpty();
        assertThat(store.abandonIndexGeneration("index", 0, START, START.plusSeconds(2),
                START.plusSeconds(3), "expired")).isPresent();
        assertThat(store.listReclaimableScalarArtifacts(FUTURE, 10)).isEmpty();
        store.markScalarArtifactDrained(late);
        assertThat(store.listReclaimableScalarArtifacts(Instant.EPOCH, 10)).isEmpty();
        assertThat(store.listReclaimableScalarArtifacts(FUTURE, 10)).containsExactly(late);
        assertThatThrownBy(() -> store.registerIndexFile(oldOwner, mapping(late)))
                .isInstanceOf(IllegalStateException.class);

        var stale = store.findIndex("index").orElseThrow();
        assertThat(store.beginIndexGeneration("index", 1, "snapshot-1", stale.updatedAt().plusSeconds(1),
                stale.updatedAt(), stale.state(), 0)).isTrue();
        assertThat(claims.tryClaim("index", 1, newOwner, START.plusSeconds(5))).isTrue();
        var mapped = intent(1, newOwner, "mapped");
        store.beginScalarArtifactWrite(mapped);
        assertThatThrownBy(() -> store.registerIndexFile(newOwner, mapping(mapped)))
                .isInstanceOf(IllegalStateException.class);
        store.markScalarArtifactDrained(mapped);
        assertThat(store.beginScalarArtifactReclaim(mapped)).isFalse();
        store.registerIndexFile(newOwner, mapping(mapped));
        assertThatThrownBy(() -> store.beginScalarArtifactWrite(mapped)).isInstanceOf(IllegalStateException.class);
        assertThat(claims.release("index", 1, newOwner)).isTrue();

        String retryOwner = UUID.randomUUID().toString();
        assertThat(claims.tryClaim("index", 1, retryOwner, START.plusSeconds(6))).isTrue();
        var first = intent(1, retryOwner, "first");
        var duplicate = intent(1, retryOwner, "mapped");
        store.beginScalarArtifactWrite(first);
        store.beginScalarArtifactWrite(duplicate);
        store.markScalarArtifactDrained(first);
        store.markScalarArtifactDrained(duplicate);
        assertThatThrownBy(() -> store.batchRegisterIndexFiles(retryOwner, List.of(mapping(first), mapping(duplicate))))
                .isInstanceOf(MetastoreException.class);
        assertThat(store.getIndexFileMappings("index")).extracting(IndexFileMapping::indexFilePath)
                .containsExactly(mapped.artifactPath());
        assertThat(claims.release("index", 1, retryOwner)).isTrue();
        assertThat(store.listReclaimableScalarArtifacts(FUTURE, 10)).containsExactlyInAnyOrder(late, first, duplicate);
        // Leave all selected work outstanding, as an unavailable store or crashed collector would.
        var firstPoll = store.pollScalarArtifactReclaimCandidates(FUTURE, 1);
        var restarted = new JdbcIndexMetadataStore(source, TableWriteAuthorityFixture.create()::requireCurrent);
        var secondPoll = restarted.pollScalarArtifactReclaimCandidates(FUTURE, 1);
        var thirdPoll = restarted.pollScalarArtifactReclaimCandidates(FUTURE, 1);
        assertThat(firstPoll).hasSize(1);
        assertThat(secondPoll).hasSize(1);
        assertThat(thirdPoll).hasSize(1);
        assertThat(List.of(firstPoll.get(0), secondPoll.get(0), thirdPoll.get(0)))
                .containsExactlyInAnyOrder(late, first, duplicate);
        assertThat(restarted.pollScalarArtifactReclaimCandidates(Instant.EPOCH, 1)).isEmpty();
        assertThat(restarted.pollScalarArtifactReclaimCandidates(FUTURE, 1)).containsExactly(firstPoll.get(0));
        assertThat(store.beginScalarArtifactReclaim(late)).isTrue();
        assertThat(store.beginScalarArtifactReclaim(late)).isTrue();
        store.completeScalarArtifactReclaim(late);
        assertThat(store.listReclaimableScalarArtifacts(FUTURE, 10)).containsExactlyInAnyOrder(first, duplicate);
        assertThat(store.beginScalarArtifactReclaim(uncertain)).isFalse();
        assertThat(store.hasScalarArtifactIntents("index")).isTrue();
        store.dropIndex("index", START.plusSeconds(10));
        assertThatThrownBy(() -> store.purgeIndex("index")).isInstanceOf(MetastoreException.class);
        assertThat(store.findIndex("index").orElseThrow().state()).isEqualTo(IndexState.DROPPED);
    }

    private static ScalarArtifactIntent intent(int generation, String owner, String source) {
        return new ScalarArtifactIntent("index", generation, owner, source,
                "/fixture/index/g" + generation + "/" + owner + "/" + source + ".idx");
    }

    private static IndexFileMapping mapping(ScalarArtifactIntent intent) {
        return new IndexFileMapping(intent.indexId(), intent.dataFilePath(), intent.artifactPath(),
                1, 10, "a".repeat(64), intent.generation(), "snapshot-" + intent.generation(), 0, START, 0);
    }
}
