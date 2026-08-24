package org.pangrova.security.authz;

import org.pangrova.security.event.SecurityPolicyEventPublisher;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.UserIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates {@link RbacRefresher} — the periodic poller that delivers cross-coordinator
 * eventual consistency for the RBAC view by re-reading the shared store on a fixed interval.
 *
 * <p>Uses an in-process {@link GrantStore} test double so the test exercises only the
 * scheduler/refresh contract, not JDBC.
 */
class RbacRefresherTest {

    private RbacRefresher refresher;
    private DynamicRbacAccessControl accessControl;

    @AfterEach
    void tearDown() {
        if (refresher != null) {
            refresher.close();
        }
    }

    private static DynamicRbacAccessControl newAc(GrantStore grantStore) {
        return new DynamicRbacAccessControl(
                grantStore, null,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()),
                new SecurityPolicyEventPublisher());
    }

    @Test
    void rejectsZeroOrNegativeInterval() {
        accessControl = newAc(new MutableGrantStore());

        assertThatThrownBy(() -> new RbacRefresher(accessControl, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RbacRefresher(accessControl, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refreshNowReloadsImmediately() {
        // Background-poll with a long interval so only refreshNow() drives behaviour here.
        MutableGrantStore grantStore = new MutableGrantStore();
        accessControl = newAc(grantStore);
        refresher = new RbacRefresher(accessControl, Duration.ofMinutes(10));

        UserIdentity user = new UserIdentity("alice", Set.of("analyst"), Map.of(), Instant.now());
        assertThatThrownBy(() ->
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders"))
                .isNotNull();

        // External actor adds a grant directly to the store (simulating a peer coordinator
        // that wrote to shared state — we have no event in this in-process double).
        grantStore.addGrant(PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));

        // Until refresh, the cache is stale.
        assertThatThrownBy(() ->
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders"))
                .isNotNull();

        refresher.refreshNow();

        assertThatCode(() ->
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders"))
                .doesNotThrowAnyException();
    }

    @Test
    void scheduledRefreshConvergesWithinPollInterval() throws InterruptedException {
        MutableGrantStore grantStore = new MutableGrantStore();
        accessControl = newAc(grantStore);
        // 100ms interval — fast enough for a unit test, still asserts the schedule fires.
        refresher = new RbacRefresher(accessControl, Duration.ofMillis(100));
        refresher.start();

        UserIdentity user = new UserIdentity("alice", Set.of("analyst"), Map.of(), Instant.now());

        assertThatThrownBy(() ->
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders"))
                .isNotNull();

        grantStore.addGrant(PrivilegeGrant.of(
                "analyst", Privilege.SELECT, "TABLE", "iceberg", "default", "orders", null));

        // Allow up to 5s for a 100ms scheduler tick to fire and converge.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        boolean granted = false;
        while (System.nanoTime() < deadline) {
            try {
                accessControl.checkCanSelectFromTable(user, "iceberg", "default", "orders");
                granted = true;
                break;
            } catch (RuntimeException ignored) {
                Thread.sleep(50);
            }
        }
        assertThat(granted)
                .as("Scheduled refresh should converge cache to store state within poll interval")
                .isTrue();
    }

    @Test
    void schedulerSurvivesTransientStoreFailure() {
        AtomicInteger calls = new AtomicInteger();
        // GrantStore that throws on the first call and recovers afterwards.
        GrantStore flakyStore = new GrantStore() {
            @Override
            public List<PrivilegeGrant> listGrants() {
                if (calls.getAndIncrement() == 0) {
                    throw new IllegalStateException("transient failure");
                }
                return List.of();
            }

            @Override
            public List<PrivilegeGrant> listGrantsForRole(String roleName) {
                return List.of();
            }

            @Override
            public void addGrant(PrivilegeGrant grant) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean revokeGrant(String grantId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int revokeGrants(String roleName, Privilege privilege,
                                    String catalog, String schema, String table) {
                throw new UnsupportedOperationException();
            }
        };

        accessControl = newAc(flakyStore);
        refresher = new RbacRefresher(accessControl, Duration.ofMillis(50));

        // Both calls swallow exceptions — a recurring failure must not cancel the schedule.
        assertThatCode(() -> {
            refresher.refreshNow();
            refresher.refreshNow();
        }).doesNotThrowAnyException();

        assertThat(calls.get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void closeStopsTheScheduler() {
        accessControl = newAc(new MutableGrantStore());
        refresher = new RbacRefresher(accessControl, Duration.ofMillis(100));
        refresher.start();
        refresher.close();

        // After close, start should fail.
        assertThatThrownBy(() -> refresher.start()).isInstanceOf(IllegalStateException.class);
    }

    /** Minimal mutable in-process GrantStore for the refresher tests. */
    private static final class MutableGrantStore implements GrantStore {
        private final List<PrivilegeGrant> grants = new ArrayList<>();

        @Override
        public synchronized List<PrivilegeGrant> listGrants() {
            return List.copyOf(grants);
        }

        @Override
        public synchronized List<PrivilegeGrant> listGrantsForRole(String roleName) {
            return grants.stream().filter(g -> g.roleName().equals(roleName)).toList();
        }

        @Override
        public synchronized void addGrant(PrivilegeGrant grant) {
            grants.add(grant);
        }

        @Override
        public synchronized boolean revokeGrant(String grantId) {
            return grants.removeIf(g -> grantId != null && grantId.equals(g.id()));
        }

        @Override
        public synchronized int revokeGrants(String roleName, Privilege privilege,
                                             String catalog, String schema, String table) {
            int before = grants.size();
            grants.removeIf(g -> g.roleName().equals(roleName) && g.privilege() == privilege);
            return before - grants.size();
        }
    }
}
