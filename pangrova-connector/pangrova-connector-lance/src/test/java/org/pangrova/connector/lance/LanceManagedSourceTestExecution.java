package org.pangrova.connector.lance;

import org.pangrova.protocol.v2.AcceptedSourceSnapshotRef;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
import org.pangrova.protocol.v2.PrepareDistributedWriteResult;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.spi.metadata.SourcePublicationReservation;
import org.pangrova.query.api.coordination.ExecutionAnchor;
import org.pangrova.connector.spi.coordination.WriteCommitEntry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/** Test-only PG/SDK execution; cross-module consumers receive only SPI, protocol and Path values. */
public final class LanceManagedSourceTestExecution {
    private final LanceManagedSourceFixture fixture;
    private final LancePgSdkTestInputs.Prepared prepared;

    private LanceManagedSourceTestExecution(LanceManagedSourceFixture fixture, LancePgSdkTestInputs.Prepared prepared) {
        this.fixture = fixture;
        this.prepared = prepared;
    }

    public static LanceManagedSourceTestExecution create(Path directory) throws Exception {
        Files.createDirectories(directory);
        var fixture = new LanceManagedSourceFixture("source-test-" + UUID.randomUUID(), directory);
        var prepared = LancePgSdkTestInputs.write(fixture, directory, directory.resolve("table.lance").toString(),
                "create-" + UUID.randomUUID(), LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 4, 7);
        return new LanceManagedSourceTestExecution(fixture, prepared);
    }

    /** Positive APPEND includes an actual accepted PG base and an official SDK prepared candidate. */
    public static LanceManagedSourceTestExecution append(Path directory) throws Exception {
        var initial = create(directory);
        var accepted = initial.accept();
        var prepared = LancePgSdkTestInputs.write(initial.fixture, directory, accepted.getDatasetUri(),
                "append-" + UUID.randomUUID(), LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND, accepted.getVersion(), 11, 13);
        return new LanceManagedSourceTestExecution(initial.fixture, prepared);
    }

    /** Production connector and exact codec over this same actual PG authority; no simulated provider. */
    public org.pangrova.connector.spi.plugin.Connector connector() { return fixture.connector(); }
    public CatalogExecutionBinding catalog() { return fixture.catalog(); }
    public ConnectorOutputTableHandle output() { return prepared.allocation().output(); }
    public WriteCommitEntry intent() { return fixture.journal().getEntry(prepared.allocation().executionId()).orElseThrow(); }
    public ExecutionAnchor originalAnchor() { return prepared.allocation().anchor(); }
    public SourcePublicationReservation reservation() { return fixture.reservation(prepared.allocation().executionId()); }
    public List<PrepareDistributedWriteResult> pages() { return prepared.pages(); }
    public PrepareDistributedWriteRequest request() throws Exception {
        return fixture.reserve(prepared.allocation().executionId(), intent().fragments());
    }
    public ExecutionAnchor handoff() { return fixture.handoff(prepared.allocation().executionId()); }
    public AcceptedSourceSnapshotRef accept() { return prepared.accept(); }
    public PrepareDistributedWriteResult preparePage(PrepareDistributedWriteRequest request) throws Exception {
        return prepared.sdk().preparePage(request);
    }
    public org.pangrova.protocol.v2.CleanupSourceObjectsResult cleanupPage(
            org.pangrova.protocol.v2.CleanupSourceObjectsRequest request) throws Exception {
        return prepared.sdk().cleanupPage(request);
    }
    public org.pangrova.protocol.v2.DatasetSchemaFingerprintResult schemaFingerprint() {
        return org.pangrova.protocol.v2.DatasetSchemaFingerprintResult.newBuilder()
                .setPhysicalSchemaSha256(prepared.allocation().binding().getLance().getSchemaFenceSha256()).build();
    }

    /** Test-only handoff of this actual container's connection, never a second metadata authority. */
    public Path databaseConnectionFile(Path destination) throws Exception {
        var actual = (org.postgresql.ds.PGSimpleDataSource) fixture.dataSource();
        var properties = new java.util.Properties();
        properties.setProperty("url", actual.getURL());
        properties.setProperty("user", actual.getUser());
        properties.setProperty("password", actual.getPassword());
        Files.createFile(destination, java.nio.file.attribute.PosixFilePermissions.asFileAttribute(
                java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
        try (var output = Files.newOutputStream(destination)) {
            properties.store(output, "Actual test container connection; never committed");
        }
        return destination;
    }
}
