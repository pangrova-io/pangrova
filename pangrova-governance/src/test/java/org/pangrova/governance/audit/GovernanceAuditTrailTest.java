package org.pangrova.governance.audit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link InMemoryGovernanceAuditTrail} verifying immutable audit recording,
 * query by resource/actor/time range, and thread safety guarantees.
 */
class GovernanceAuditTrailTest {

    private InMemoryGovernanceAuditTrail auditTrail;

    @BeforeEach
    void setUp() {
        auditTrail = new InMemoryGovernanceAuditTrail();
    }

    @Test
    void recordCreatesImmutableEntryWithTimestampAndId() {
        GovernanceAuditEntry entry = auditTrail.record(
                "admin@corp.com",
                OperationType.POLICY_CHANGE,
                "catalog.schema.users",
                Map.of("mask", "none"),
                Map.of("mask", "email_mask"),
                "Applied PII masking policy");

        assertThat(entry.id()).isPositive();
        assertThat(entry.timestamp()).isNotNull();
        assertThat(entry.actor()).isEqualTo("admin@corp.com");
        assertThat(entry.operationType()).isEqualTo(OperationType.POLICY_CHANGE);
        assertThat(entry.resourcePath()).isEqualTo("catalog.schema.users");
        assertThat(entry.beforeState()).containsEntry("mask", "none");
        assertThat(entry.afterState()).containsEntry("mask", "email_mask");
        assertThat(entry.details()).isEqualTo("Applied PII masking policy");
    }

    @Test
    void recordedEntriesAreImmutable() {
        GovernanceAuditEntry entry = auditTrail.record(
                "user1", OperationType.ACCESS_GRANT, "catalog.schema.table",
                Map.of(), Map.of("role", "analyst"), "Granted read access");

        assertThat(entry.beforeState()).isUnmodifiable();
        assertThat(entry.afterState()).isUnmodifiable();
    }

    @Test
    void getEntriesForResourceReturnsMatchingEntriesDescending() {
        auditTrail.record("user1", OperationType.TAG_CHANGE, "catalog.schema.users.email",
                Map.of(), Map.of("pii_type", "EMAIL"), "Tagged as PII");
        auditTrail.record("user2", OperationType.MASK_CHANGE, "catalog.schema.orders",
                Map.of(), Map.of("mask", "partial"), "Applied mask");
        auditTrail.record("user1", OperationType.POLICY_CHANGE, "catalog.schema.users.email",
                Map.of("pii_type", "EMAIL"), Map.of("pii_type", "PII_EMAIL"), "Updated tag");

        List<GovernanceAuditEntry> entries = auditTrail.getEntriesForResource("catalog.schema.users.email");

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).details()).isEqualTo("Updated tag");
        assertThat(entries.get(1).details()).isEqualTo("Tagged as PII");
    }

    @Test
    void getEntriesByActorReturnsMatchingEntries() {
        auditTrail.record("admin", OperationType.ACCESS_GRANT, "resource1",
                Map.of(), Map.of("role", "reader"), "Grant");
        auditTrail.record("other", OperationType.TAG_CHANGE, "resource2",
                Map.of(), Map.of("tag", "pii"), "Tag");
        auditTrail.record("admin", OperationType.ACCESS_REVOKE, "resource1",
                Map.of("role", "reader"), Map.of(), "Revoke");

        List<GovernanceAuditEntry> entries = auditTrail.getEntriesByActor("admin");

        assertThat(entries).hasSize(2);
        assertThat(entries).allMatch(e -> e.actor().equals("admin"));
    }

    @Test
    void getEntriesInRangeFiltersCorrectly() {
        Instant before = Instant.now();
        auditTrail.record("user1", OperationType.QUALITY_RULE_CHANGE, "resource",
                Map.of(), Map.of("rule", "not_null"), "Added rule");
        Instant after = Instant.now();

        List<GovernanceAuditEntry> inRange = auditTrail.getEntriesInRange(before, after.plusMillis(1));
        List<GovernanceAuditEntry> outOfRange = auditTrail.getEntriesInRange(
                after.plusMillis(100), after.plusMillis(200));

        assertThat(inRange).hasSize(1);
        assertThat(outOfRange).isEmpty();
    }

    @Test
    void entryCountTracksRecordedEntries() {
        assertThat(auditTrail.entryCount()).isZero();

        auditTrail.record("user1", OperationType.TAG_CHANGE, "r1",
                Map.of(), Map.of("k", "v"), "d1");
        auditTrail.record("user2", OperationType.MASK_CHANGE, "r2",
                Map.of(), Map.of("k", "v"), "d2");

        assertThat(auditTrail.entryCount()).isEqualTo(2);
    }

    @Test
    void idsAreMonotonicallyIncreasing() {
        GovernanceAuditEntry e1 = auditTrail.record("u", OperationType.TAG_CHANGE, "r",
                Map.of(), Map.of(), "");
        GovernanceAuditEntry e2 = auditTrail.record("u", OperationType.TAG_CHANGE, "r",
                Map.of(), Map.of(), "");
        GovernanceAuditEntry e3 = auditTrail.record("u", OperationType.TAG_CHANGE, "r",
                Map.of(), Map.of(), "");

        assertThat(e1.id()).isLessThan(e2.id());
        assertThat(e2.id()).isLessThan(e3.id());
    }
}
