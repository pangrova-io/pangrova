package org.pangrova.connector.lance;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.connector.api.enums.ChangeSetMode;
import org.pangrova.connector.api.materialization.MaterializationCapabilities;
import org.pangrova.connector.api.exception.MaterializationSourceException;
import org.pangrova.connector.api.materialization.PinnedSource;
import org.pangrova.connector.api.materialization.SourceProvider;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.connector.api.materialization.SourceSnapshotRef;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exact accepted-version pinning over the real PostgreSQL source authority. */
class LanceMaterializationSourceProviderTest extends ContainerTestBase {
    private static final String TENANT = "tenant-a";
    private static final String POLICY = "policy:materialization-v1";

    @TempDir Path warehouse;
    private String catalog;
    private LanceManagedSourceFixture fixture;
    private SourceProvider provider;

    @BeforeEach void setUp() {
        catalog = "catalog_" + UUID.randomUUID();
        fixture = new LanceManagedSourceFixture(catalog, warehouse);
        fixture.store().createSchema(catalog, TENANT, "db");
        provider = fixture.connector().getMaterializationSourceProvider().orElseThrow();
    }

    @Test
    void describesAndPinsTheExactAcceptedVersion() throws Exception {
        var prepared = write("pinned", "pinned", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1, 2);
        var table = prepared.allocation().output().table();
        prepared.accept();
        var source = source(table);

        SourceSnapshotRef described = provider.describe(source, TENANT, "1", POLICY);

        assertThat(described.connectorId()).isEqualTo("lance");
        assertThat(described.snapshotToken()).isEqualTo("1");
        assertThat(described.schemaFingerprint())
                .isEqualTo("lance-schema:" + HexFormat.of().formatHex(table.schemaFence()));
        assertThat(described.policyFingerprint()).isEqualTo(POLICY);
        assertThat(described.observedAt()).isBeforeOrEqualTo(Instant.now());
        assertThat(described.pinnable()).isTrue();

        PinnedSource pinned = provider.pin(source, TENANT, described, "attempt-1");
        assertThat(pinned.tenant()).isEqualTo(TENANT);
        assertThat(pinned.source()).isEqualTo(source);
        assertThat(pinned.snapshot()).isEqualTo(described);
        assertThat(pinned.pinOwner()).isEqualTo("attempt-1");
        provider.unpin(pinned);
    }

    @Test
    void historicalVersionRemainsPinnableAfterTheHeadAdvances() throws Exception {
        var first = write("history", "history-first", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1);
        var table = first.allocation().output().table();
        first.accept();
        var second = write("history", "history-second", LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND, 1, 2);
        second.accept();
        var source = source(table);

        SourceSnapshotRef versionOne = provider.describe(source, TENANT, "1", POLICY);
        PinnedSource pinned = provider.pin(source, TENANT, versionOne, "attempt-1");

        assertThat(pinned.snapshot().snapshotToken()).isEqualTo("1");
        assertThat(provider.describe(source, TENANT, "2", POLICY).snapshotToken()).isEqualTo("2");
    }

    @Test
    void failsClosedWhenTheVersionIsAbsentEmptyOrMalformed() throws Exception {
        var prepared = write("absent", "absent", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1);
        var table = prepared.allocation().output().table();
        prepared.accept();
        var source = source(table);

        assertReason(() -> provider.describe(source, TENANT, "2", POLICY),
                MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE);
        assertReason(() -> provider.describe(source, TENANT, "0", POLICY),
                MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE);
        assertReason(() -> provider.describe(source, TENANT, "not-a-version", POLICY),
                MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE);
        assertReason(() -> provider.describe(source, "tenant-b", "1", POLICY),
                MaterializationSourceException.Reason.SOURCE_UNAVAILABLE);
        assertReason(() -> provider.describe(
                        new SourceRef("lance", catalog, "db", "missing"), TENANT, "1", POLICY),
                MaterializationSourceException.Reason.SOURCE_UNAVAILABLE);
    }

    @Test
    void rejectsASnapshotWhoseSchemaChangedSincePlanning() throws Exception {
        var prepared = write("shape", "shape", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1);
        var table = prepared.allocation().output().table();
        prepared.accept();
        var source = source(table);
        var expected = new SourceSnapshotRef("lance", "1", "lance-schema:drifted", POLICY,
                Instant.now(), true);

        assertReason(() -> provider.pin(source, TENANT, expected, "attempt-1"),
                MaterializationSourceException.Reason.SHAPE_MISMATCH);
        assertThatThrownBy(() -> provider.pin(source, TENANT,
                new SourceSnapshotRef("iceberg", "1", expected.schemaFingerprint(), POLICY,
                        Instant.now(), true), "attempt-1"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void droppedIncarnationCannotResolveItsOldVersion() throws Exception {
        var prepared = write("dropped", "dropped", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1);
        var table = prepared.allocation().output().table();
        prepared.accept();
        var source = source(table);
        var current = fixture.store().load(catalog, TENANT, table.table());
        fixture.store().drop(current);
        var replacement = new LanceTableHandle(table.catalog(), table.tenantId(), table.table(),
                UUID.randomUUID().toString(), table.datasetUri(), table.columns(), Map.of(),
                table.schemaFence(), 0, 1, 0);
        fixture.store().create(replacement);

        assertReason(() -> provider.describe(source, TENANT, "1", POLICY),
                MaterializationSourceException.Reason.SNAPSHOT_UNAVAILABLE);
    }

    @Test
    void declaresNoChangeSetCapabilityAndFailsClosed() throws Exception {
        var prepared = write("caps", "caps", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1);
        var table = prepared.allocation().output().table();
        prepared.accept();
        var source = source(table);

        MaterializationCapabilities capabilities = provider.capabilities(source);
        assertThat(capabilities.snapshotPin()).isTrue();
        assertThat(capabilities.internalNamespace()).isTrue();
        assertThat(capabilities.idempotentCommitLookup()).isTrue();
        assertThat(capabilities.appendOnlyProof()).isFalse();
        assertThat(capabilities.changeSet()).isFalse();
        assertThat(capabilities.supports(ChangeSetMode.APPEND_ONLY)).isFalse();
        assertThat(capabilities.supports(ChangeSetMode.PARTITION_REPLACE)).isFalse();

        var pinned = provider.pin(source, TENANT, provider.describe(source, TENANT, "1", POLICY),
                "attempt-1");
        assertReason(() -> provider.changeSet(pinned, pinned, ChangeSetMode.APPEND_ONLY),
                MaterializationSourceException.Reason.CHANGESET_UNSUPPORTED);
        assertThatThrownBy(() -> provider.capabilities(new SourceRef("iceberg", catalog, "db", "x")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static SourceRef source(LanceTableHandle table) {
        return new SourceRef("lance", table.catalog(), table.table().schema(), table.table().table());
    }

    private LancePgSdkTestInputs.Prepared write(String dataset, String writeId,
            LanceWriteOperation operation, long base, int... values) throws Exception {
        return LancePgSdkTestInputs.write(fixture, warehouse, warehouse.resolve(dataset + ".lance").toString(),
                writeId, operation, base, values);
    }

    private static void assertReason(Runnable action, MaterializationSourceException.Reason reason) {
        assertThatThrownBy(action::run)
                .isInstanceOf(MaterializationSourceException.class)
                .satisfies(failure -> assertThat(
                        ((MaterializationSourceException) failure).reason()).isEqualTo(reason));
    }
}
