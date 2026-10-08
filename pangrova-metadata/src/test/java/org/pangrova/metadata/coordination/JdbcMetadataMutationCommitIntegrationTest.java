package org.pangrova.metadata.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.store.JdbcMetadataStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.postgresql.ds.PGSimpleDataSource;

import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Reuses the actual PostgreSQL wire-fault boundary; no Connection or SQL results are replaced. */
class JdbcMetadataMutationCommitIntegrationTest {

    @Test
    void lostActualCommitAckCannotBeReportedAsSuccessAndRetainsRollbackFailure() throws Exception {
        var primary = (PGSimpleDataSource) SharedPostgresContainer.migratedDataSource("metadata_commit_ack");
        var descriptor = new CatalogDescriptor("commit_ack", CatalogType.FEDERATION, "inventory",
                CatalogState.ACTIVE, Map.of("version", "one"), "", Set.of());
        try (var wire = new JournalCommitWireFault(primary.getServerNames()[0], primary.getPortNumbers()[0])) {
            var relayed = new PGSimpleDataSource();
            relayed.setURL(primary.getURL());
            relayed.setServerNames(new String[] {InetAddress.getLoopbackAddress().getHostAddress()});
            relayed.setPortNumbers(new int[] {wire.port()});
            relayed.setUser(primary.getUser());
            relayed.setPassword(primary.getPassword());
            relayed.setCurrentSchema(primary.getCurrentSchema());
            relayed.setSslMode("disable");
            relayed.setGssEncMode("disable");
            var relayedStore = new JdbcMetadataStore(relayed);
            wire.armCommitResponseLoss();
            try {
                Throwable failure = catchThrowable(() -> relayedStore.saveCatalogDescriptor(descriptor));

                assertThat(failure).isInstanceOf(MetastoreException.class)
                        .hasMessageContaining("commit outcome unconfirmed")
                        .hasMessageContaining("cacheKey=commit_ack");
                assertThat(failure.getCause()).isInstanceOf(SQLException.class);
                assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("08006");
                assertThat(failure.getSuppressed()).anySatisfy(rollback -> {
                    assertThat(rollback).isInstanceOf(SQLException.class);
                    assertThat(((SQLException) rollback).getSQLState()).isEqualTo("08003");
                });
                assertThat(wire.awaitActualCommit()).isTrue();

                // This independent real observer, not the failed return, establishes the committed facts.
                var observer = new JdbcMetadataStore(primary);
                assertThat(observer.getCatalogDescriptor(descriptor.name())).contains(descriptor);
                assertThat(observer.findCatalogDescriptorRevision(
                        descriptor.name(), descriptor.incarnationId(), descriptor.revision())).contains(descriptor);
                try (var connection = primary.getConnection(); var statement = connection.createStatement();
                        var rows = statement.executeQuery(
                                "SELECT cache_key, invalidation_type FROM invalidation_log ORDER BY id")) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getString(1)).isEqualTo(descriptor.name());
                    assertThat(rows.getString(2)).isEqualTo("CATALOG");
                    assertThat(rows.next()).isFalse();
                }
            } finally {
                wire.allowReconciliation();
            }
        }
    }

    @Test
    void actualDeferredCommitRejectionPreservesSqlCauseAndRollsBackBothFacts() throws Exception {
        var primary = SharedPostgresContainer.migratedDataSource("metadata_commit_reject");
        var store = new JdbcMetadataStore(primary);
        var descriptor = new CatalogDescriptor("commit_reject", CatalogType.FEDERATION, "inventory",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        try (var connection = primary.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION reject_metadata_commit() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN RAISE EXCEPTION 'commit invalidation denied' USING ERRCODE='42501'; END $$");
            statement.execute("CREATE CONSTRAINT TRIGGER reject_metadata_commit AFTER INSERT ON invalidation_log "
                    + "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_metadata_commit()");
        }

        Throwable failure = catchThrowable(() -> store.saveCatalogDescriptor(descriptor));

        assertThat(failure).isInstanceOf(MetastoreException.class)
                .hasMessageContaining("commit outcome unconfirmed").hasMessageContaining("sqlState=42501");
        assertThat(failure.getCause()).isInstanceOf(SQLException.class);
        assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("42501");
        assertThat(store.getCatalogDescriptor(descriptor.name())).isEmpty();
        assertThat(store.findCatalogDescriptorRevision(
                descriptor.name(), descriptor.incarnationId(), descriptor.revision())).isEmpty();
        try (var connection = primary.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT count(*) FROM invalidation_log")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong(1)).isZero();
        }
    }
}
