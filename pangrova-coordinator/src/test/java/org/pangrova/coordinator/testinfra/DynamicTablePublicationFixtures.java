package org.pangrova.coordinator.testinfra;

import org.pangrova.metadata.coordination.JdbcDistributedLease;
import org.pangrova.metadata.coordination.JdbcExecutionAnchorStore;
import org.pangrova.metadata.coordination.JdbcWriteCommitJournal;
import org.pangrova.metadata.dynamictable.DynamicTableDefinition;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshAttempt;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshIssuer;
import org.pangrova.metadata.dynamictable.JdbcDynamicTablePublicationStore;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitFingerprint;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/** Real PostgreSQL candidate authority; journal records are component oracles, not Native execution. */
public final class DynamicTablePublicationFixtures {
    private DynamicTablePublicationFixtures() {}

    public static DynamicTableRefreshIssuer issuer(DataSource dataSource) {
        var leases = new JdbcDistributedLease(dataSource);
        var leader = leases.currentHolder("coordinator_leader").orElseGet(() -> leases.tryAcquire(
                "coordinator_leader", "component-coordinator", Duration.ofMinutes(5)).orElseThrow());
        var owner = new JdbcExecutionAnchorStore(dataSource).tryAcquire("dt-" + UUID.randomUUID(),
                leader.holderId(), "component-process", Duration.ofMinutes(5)).orElseThrow();
        return new DynamicTableRefreshIssuer(owner, leader.epoch(), "component-user");
    }

    public static DynamicTableRefreshAttempt create(DataSource dataSource, String tenant, String logical,
            String definition, String catalog, String schema) {
        var store = new JdbcDynamicTablePublicationStore(dataSource);
        var reserved = store.create(new DynamicTableDefinition(tenant, logical, definition, catalog, schema),
                issuer(dataSource));
        return publish(dataSource, reserved.attempt());
    }

    public static DynamicTableRefreshAttempt publish(DataSource dataSource, DynamicTableRefreshAttempt attempt) {
        String[] target = attempt.candidateTable().split("\\.");
        var receipt = CoordinatorWriteReceiptFixtures.receipt();
        List<byte[]> fragments = List.of(new byte[] {1});
        byte[] binding = {2};
        var fingerprint = WriteCommitFingerprint.compute(new WriteCommitFingerprint.Intent(attempt.executionId(),
                attempt.tenantId(), target[0], target[1] + "." + target[2], "INSERT", -1, binding,
                attempt.issuer().principal(), receipt, null), fragments);
        var owner = attempt.issuer().owner();
        var journal = new JdbcWriteCommitJournal(dataSource);
        journal.recordIntent(new WriteCommitEntry(attempt.executionId(), target[0], target[1] + "." + target[2],
                WriteCommitEntry.State.PENDING, System.currentTimeMillis(), fragments, "INSERT", fingerprint,
                attempt.tenantId(), attempt.issuer().principal(), receipt, -1, binding, fingerprint,
                owner.ownerId(), owner.ownerIncarnation(), owner.fencingToken(), null, null));
        journal.recordPublication(attempt.executionId(), fingerprint,
                new NativeWriteFinalization(NativeWriteFinalization.Outcome.COMMITTED, 101, new byte[32]));
        journal.markCommitted(attempt.executionId());
        return new JdbcDynamicTablePublicationStore(dataSource).publish(attempt, attempt.issuer());
    }
}
