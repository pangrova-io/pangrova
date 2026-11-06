package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests exact backend selection and a real relational lease, without changing process configuration. */
@ResourceLock("SYSTEM_PROPERTIES")
class CoordinatorLeaseBootstrapTest {
    private static final String BACKEND = "pangrova.coordination.lease.backend";
    private static final String CONNECT = "pangrova.coordination.lease.zookeeper.connectString";
    private final Map<String, String> previous = new HashMap<>();

    @BeforeEach
    void rememberProperties() {
        previous.put(BACKEND, System.getProperty(BACKEND));
        previous.put(CONNECT, System.getProperty(CONNECT));
    }

    @AfterEach
    void restoreProperties() {
        previous.forEach((key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
        });
    }

    @Test
    void unknownBackendFailsInsteadOfSelectingJdbc() {
        System.setProperty(BACKEND, "mistyped-quorum");
        assertThatThrownBy(() -> CoordinatorLeaseBootstrap.resolve(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown coordination lease backend");
    }

    @Test
    void quorumRequiresAnExplicitNonblankEndpoint() {
        for (String backend : new String[] {"zookeeper", "zk"}) {
            System.setProperty(BACKEND, backend);
            System.clearProperty(CONNECT);
            assertThatThrownBy(() -> CoordinatorLeaseBootstrap.resolve(null))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("connectString");
            System.setProperty(CONNECT, "   ");
            assertThatThrownBy(() -> CoordinatorLeaseBootstrap.resolve(null))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("connectString");
        }
    }

    @Test
    void jdbcRequiresTheActualMetadataDataSource() {
        System.setProperty(BACKEND, "jdbc");
        assertThatThrownBy(() -> CoordinatorLeaseBootstrap.resolve(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("dataSource");
    }

    @Test
    void defaultAndExplicitJdbcShareTheRealRelationalLeaseAndFencing() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("bootstrap_lease_selection");
        System.clearProperty(BACKEND);
        var first = CoordinatorLeaseBootstrap.resolve(dataSource);
        System.setProperty(BACKEND, " JDBC ");
        var second = CoordinatorLeaseBootstrap.resolve(dataSource);
        assertThat(first.quorumProvider()).isNull();
        assertThat(second.quorumProvider()).isNull();
        var acquired = first.lease().tryAcquire("actual-lease", "owner-one", Duration.ofSeconds(30)).orElseThrow();
        assertThat(second.lease().tryAcquire("actual-lease", "owner-two", Duration.ofSeconds(30))).isEmpty();
        assertThat(first.lease().release(acquired.leaseKey(), acquired.holderId(), acquired.epoch())).isTrue();
        var replacement = second.lease().tryAcquire("actual-lease", "owner-two", Duration.ofSeconds(30)).orElseThrow();
        assertThat(replacement.epoch()).isGreaterThan(acquired.epoch());
        assertThat(first.lease().release(acquired.leaseKey(), acquired.holderId(), acquired.epoch())).isFalse();
        assertThat(second.lease().release(replacement.leaseKey(), replacement.holderId(), replacement.epoch())).isTrue();
    }
}
