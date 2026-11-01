package org.pangrova.coordinator.coordination.copy;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.JdbcDistributedLease;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.query.transaction.JdbcCopySourceReadAdmissions;
import org.pangrova.spi.security.AccessDeniedException;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.coordination.copy.JdbcCopySourceReadAdmissionsTest.fixture;
import static org.pangrova.coordinator.coordination.copy.JdbcCopySourceReadAdmissionsTest.inputAttempt;
import static org.pangrova.coordinator.coordination.copy.JdbcCopySourceReadAdmissionsTest.stageRoot;

/** Real PostgreSQL lineage, concurrent delivery and migration boundaries. */
class JdbcCopyFrozenInputLineageTest {
    @Test
    void concurrentDeliveryGetsDistinctNoncesForOneImmutableScope() throws Exception {
        var fixture = fixture("copy_input_concurrent");
        var proof = stageRoot(fixture);
        var attempt = inputAttempt(fixture, "concurrent-leaf");
        var executor = Executors.newFixedThreadPool(4);
        var start = new CountDownLatch(1);
        try {
            var results = new java.util.ArrayList<java.util.concurrent.Future<JdbcCopySourceReadAdmissions.FrozenInput>>();
            for (int i = 0; i < 4; i++) {
                results.add(executor.submit(() -> {
                    if (!start.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("No start signal");
                    return fixture.admissions().issueFrozenInput(attempt, fixture.anchor());
                }));
            }
            start.countDown();
            var nonces = new java.util.HashSet<String>();
            for (var result : results) {
                var input = result.get(10, TimeUnit.SECONDS);
                assertThat(nonces.add(input.ticket().nonce())).isTrue();
                assertThat(fixture.redeem(input.ticket(), input.scope()).stagedSource()).contains(proof);
            }
            assertThat(nonces).hasSize(4);
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void issuanceGateSerializesOneAttemptWithoutBlockingAnother() throws Exception {
        var fixture = fixture("copy_input_gate_scope");
        var proof = stageRoot(fixture);
        var blocked = inputAttempt(fixture, "blocked-leaf");
        var independent = inputAttempt(fixture, "independent-leaf");
        var executor = Executors.newFixedThreadPool(2);
        try (var gate = fixture.source().getConnection()) {
            gate.setAutoCommit(false);
            try (var query = gate.prepareStatement("""
                    SELECT pg_advisory_xact_lock(207578818,
                        hashtext(json_build_array(?::text,?::text,?::text)::text))
                    """)) {
                query.setString(1, blocked.tenantId());
                query.setString(2, blocked.executionId());
                query.setString(3, blocked.taskId());
                query.execute();
            }
            var waiting = executor.submit(() -> fixture.admissions().issueFrozenInput(blocked, fixture.anchor()));
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
                try (var connection = fixture.source().getConnection(); var query = connection.prepareStatement(
                        "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=207578818 AND NOT granted");
                        var rows = query.executeQuery()) {
                    return rows.next() && rows.getLong(1) == 1;
                }
            });
            assertThat(waiting.isDone()).isFalse();
            var ready = executor.submit(() -> fixture.admissions().issueFrozenInput(independent, fixture.anchor()))
                    .get(5, TimeUnit.SECONDS);
            assertThat(fixture.redeem(ready.ticket(), ready.scope()).stagedSource()).contains(proof);
            gate.commit();
            assertThat(waiting.get(5, TimeUnit.SECONDS).proof()).isEqualTo(proof);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void directSqlCannotRewriteInheritedProofOrParentBeforeWorkerRedemption() throws Exception {
        var fixture = fixture("copy_input_immutable");
        stageRoot(fixture);
        fixture.admissions().issueFrozenInput(inputAttempt(fixture, "leaf-one"), fixture.anchor());
        try (var connection = fixture.source().getConnection(); var statement = connection.createStatement()) {
            for (String set : new String[]{"parent_nonce_sha256=NULL", "staged_content_sha256=repeat('c',64)",
                    "staged_source_version_id='replacement'", "staged_content_size=124"}) {
                assertThatThrownBy(() -> statement.executeUpdate(
                        "UPDATE copy_source_read_admissions SET " + set + " WHERE staging_attempt_id='leaf-one'"))
                        .isInstanceOf(SQLException.class).hasMessageContaining("immutable");
            }
            assertThatThrownBy(() -> statement.executeUpdate("""
                    INSERT INTO copy_source_read_admissions
                    SELECT (jsonb_populate_record(NULL::copy_source_read_admissions,
                        to_jsonb(a) || jsonb_build_object('nonce_sha256', repeat('c',64),
                            'staging_attempt_id','injected','staged_content_size',999))).*
                    FROM copy_source_read_admissions a WHERE staging_attempt_id='leaf-one'
                    """)).isInstanceOf(SQLException.class).hasMessageContaining("exact frozen root proof");
        }
    }

    @Test
    void controlFailoverNeedsNewTaskIdentityAndDoesNotRepinItsSource() throws Exception {
        var fixture = fixture("copy_input_control_takeover");
        var proof = stageRoot(fixture);
        var old = inputAttempt(fixture, "old-leaf");
        var bound = fixture.admissions().issueFrozenInput(old, fixture.anchor());
        var leases = new JdbcDistributedLease(fixture.source());
        assertThat(leases.release("coordinator_leader", fixture.scope().ownerId(), old.controlEpoch())).isTrue();
        var leader = leases.tryAcquire("coordinator_leader", "replacement", Duration.ofMinutes(5)).orElseThrow();
        assertThatThrownBy(() -> fixture.admissions().issueFrozenInput(old, fixture.anchor()))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> fixture.redeem(bound.ticket(), bound.scope()))
                .isInstanceOf(AccessDeniedException.class);
        var current = new JdbcCopySourceReadAdmissions.FrozenInputAttempt(old.tenantId(), old.principal(),
                old.executionId(), old.sourceAttemptId(), "new-leaf", leader.epoch(), old.deadlineEpochMs());
        var resumed = fixture.admissions().issueFrozenInput(current, fixture.anchor());
        assertThat(fixture.redeem(resumed.ticket(), resumed.scope()).stagedSource()).contains(proof);
    }

}
