package org.pangrova.metadata.file;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.security.store.jdbc.AuthorizationTransactionGate;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.pangrova.security.api.tenant.TenantRole;
import org.pangrova.security.api.tenant.TenantStatus;
import org.pangrova.security.api.AccessDeniedException;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceAuthority;
import org.pangrova.file.store.jdbc.space.JdbcFileSpaceAuthority;
import org.pangrova.common.file.FileSpaceType;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class JdbcFileSpaceAuthorityTest {
    private static final UserIdentity ALICE = new UserIdentity("alice", Set.of("users"), Map.of(), Instant.EPOCH);
    private DataSource source;
    private JdbcTenantStore tenants;
    private JdbcFileSpaceAuthority authority;

    @BeforeEach
    void setup() throws Exception {
        source = SharedPostgresContainer.migratedDataSource("file_subject_authority");
        tenants = new JdbcTenantStore(source);
        tenants.initSchema();
        tenants.createTenant(tenant(TenantStatus.ACTIVE, List.of("cat"), List.of("db")));
        tenants.addUserToTenant("tenant-a", "alice", TenantRole.MEMBER);
        authority = new JdbcFileSpaceAuthority(source, tenants);
    }

    @Test
    void claimlessAuthenticationResolvesActualMembershipAndPreservesAuthenticatedSubject() {
        var resolved = authority.read(() -> authority.resolve(ALICE, space("cat", "db")));
        assertThat(resolved.principal()).isEqualTo(ALICE.principal());
        assertThat(resolved.groups()).isEqualTo(ALICE.groups());
        assertThat(resolved.authenticatedAt()).isEqualTo(ALICE.authenticatedAt());
        assertThat(resolved.attributes()).containsEntry("tenant_id", "tenant-a");
        assertThat(ALICE.attributes()).isEmpty();
    }

    @Test
    void claimsCannotInventMembershipOrRetainAFormerTenant() {
        var unmapped = new UserIdentity("unknown", Set.of(), Map.of("tenant_id", "tenant-a"), Instant.EPOCH);
        var stale = new UserIdentity("alice", Set.of(), Map.of("tenant_id", "tenant-b"), Instant.EPOCH);
        assertThatThrownBy(() -> authority.read(() -> authority.resolve(unmapped, space("cat", "db"))))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("membership");
        assertThatThrownBy(() -> authority.read(() -> authority.resolve(stale, space("cat", "db"))))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("claim");
        tenants.removeUserFromTenant("tenant-a", "alice");
        assertThatThrownBy(() -> authority.read(() -> authority.resolve(ALICE, space("cat", "db"))))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("membership");
    }

    @Test
    void inactiveTenantsAndNamespacesOutsideTheirCurrentAllowlistAreDenied() {
        for (FileSpace outside : List.of(space("other", "db"), space("cat", "other"))) {
            assertThatThrownBy(() -> authority.read(() -> authority.resolve(ALICE, outside)))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("namespace");
        }
        for (TenantStatus status : List.of(TenantStatus.SUSPENDED, TenantStatus.DELETED)) {
            tenants.updateTenant(tenant(status, List.of(), List.of()));
            assertThatThrownBy(() -> authority.read(() -> authority.resolve(ALICE, space("cat", "db"))))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("ACTIVE");
        }
    }

    @Test
    void authorityMutationCannotOvertakeAnIssuanceAlreadyInsideItsReadBoundary() throws Exception {
        var entered = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var issuance = CompletableFuture.supplyAsync(() -> authority.read(() -> {
                var subject = authority.resolve(ALICE, space("cat", "db"));
                entered.countDown();
                try {
                    if (!finish.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Issuance not released");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return subject;
            }), executor);
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var revoke = CompletableFuture.runAsync(() -> tenants.removeUserFromTenant("tenant-a", "alice"), executor);
            await().atMost(Duration.ofSeconds(5)).until(() -> waiting("DELETE FROM tenant_user_memberships%"));
            assertThat(revoke.isDone()).isFalse();
            finish.countDown();
            assertThat(issuance.get(5, TimeUnit.SECONDS).attributes()).containsEntry("tenant_id", "tenant-a");
            revoke.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(() -> authority.read(() -> authority.resolve(ALICE, space("cat", "db"))))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("membership");
        } finally {
            finish.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void queuedIssuanceSeesACommittedRevocation() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var mutation = source.getConnection()) {
            mutation.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(mutation);
            try (var remove = mutation.prepareStatement("DELETE FROM tenant_user_memberships WHERE user_principal='alice'")) {
                remove.executeUpdate();
            }
            var pending = CompletableFuture.supplyAsync(() -> authority.read(
                    () -> authority.resolve(ALICE, space("cat", "db"))), executor);
            await().atMost(Duration.ofSeconds(5)).until(() -> waiting("SELECT pg_advisory_xact_lock_shared%"));
            assertThat(pending.isDone()).isFalse();
            mutation.commit();
            assertThatThrownBy(() -> pending.get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(AccessDeniedException.class);
        } finally {
            executor.shutdownNow();
        }
    }

    private boolean waiting(String sql) throws Exception {
        try (var connection = source.getConnection(); var query = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event='advisory' AND query LIKE ?)");) {
            query.setString(1, sql);
            try (var row = query.executeQuery()) {
                assertThat(row.next()).isTrue();
                return row.getBoolean(1);
            }
        }
    }

    private static Tenant tenant(TenantStatus status, List<String> catalogs, List<String> schemas) {
        return new Tenant("tenant-a", "File tenant", status, new ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0),
                catalogs, schemas, "", Map.of(), Instant.EPOCH, "fixture");
    }

    private static FileSpace space(String catalog, String schema) {
        return new FileSpace(UUID.randomUUID(), FileSpaceType.STAGE, catalog, schema, "files", false,
                "local", "", "root", false, "", "alice", Instant.EPOCH, Instant.EPOCH);
    }
}
