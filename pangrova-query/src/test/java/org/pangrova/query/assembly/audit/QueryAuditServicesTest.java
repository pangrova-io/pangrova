package org.pangrova.query.assembly.audit;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.query.audit.QueryAuditRecord;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class QueryAuditServicesTest {
    @Test
    void profilesReadTheSameRecordAdmittedByTheirAuditService() {
        var services = new QueryAuditServices();
        var record = record("profile-query");
        services.auditService().persist(record);
        assertThat(services.auditService().findByQueryId(record.queryId())).contains(record);
        assertThat(services.profileService().getProfile(record.queryId())).get()
                .extracting(profile -> profile.queryId()).isEqualTo(record.queryId());
    }

    @Test
    void independentCoordinatorsDoNotShareAuditOrProfileState() {
        var first = new QueryAuditServices();
        var second = new QueryAuditServices();
        first.auditService().persist(record("same-query"));
        assertThat(second.auditService().findByQueryId("same-query")).isEmpty();
        assertThat(second.profileService().getProfile("same-query")).isEmpty();
    }

    private static QueryAuditRecord record(String queryId) {
        return new QueryAuditRecord(queryId, "user", "SELECT 1", "tenant", 1, 1,
                1, 1, 1, 0, 1, 1, 1, 1, 0, 0, Instant.parse("2026-01-01T00:00:00Z"), true);
    }
}
