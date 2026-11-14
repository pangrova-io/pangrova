package org.pangrova.coordinator.integration.metadata.property.service;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;

import org.pangrova.metadata.catalog.CatalogMetadata;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.StringLength;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;

import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for metadata query-level transaction idempotency.
 *
 * <p><b>Validates: Requirements 6.1, 6.5</b></p>
 */
class MetadataTransactionPropertyTest {

    /**
     * Property 7: beginQuery/cleanupQuery idempotency — beginQuery.
     *
     * <p>For any queryId, calling beginQuery multiple times must delegate
     * to the connector exactly once.</p>
     */
    @Property(tries = 10)
    void beginQueryIsIdempotent(
            @ForAll @AlphaChars @StringLength(min = 1, max = 30) String queryId) {
        CountingConnectorMetadata counting = new CountingConnectorMetadata();
        CatalogMetadata service = buildService(counting);
        var query = new org.pangrova.coordinator.task.TestConnectorQuery(queryId);

        service.beginQuery(query.metadataSession());
        service.beginQuery(query.metadataSession());
        service.beginQuery(query.metadataSession());

        assertThat(counting.beginCount.get())
                .as("beginQuery should delegate exactly once per queryId")
                .isEqualTo(1);
    }

    /**
     * Property 7: cleanupQuery idempotency.
     */
    @Property(tries = 10)
    void cleanupQueryIsIdempotent(
            @ForAll @AlphaChars @StringLength(min = 1, max = 30) String queryId) {
        CountingConnectorMetadata counting = new CountingConnectorMetadata();
        CatalogMetadata service = buildService(counting);
        var query = new org.pangrova.coordinator.task.TestConnectorQuery(queryId);

        service.beginQuery(query.metadataSession());
        query.seal();
        service.cleanupQuery(query.authority());
        query.seal();
        service.cleanupQuery(query.authority());
        query.seal();
        service.cleanupQuery(query.authority());

        assertThat(counting.cleanupCount.get())
                .as("cleanupQuery should delegate exactly once per queryId")
                .isEqualTo(1);
    }

    /**
     * Property 7: cleanupQuery on never-begun queryId is a no-op.
     */
    @Property(tries = 10)
    void cleanupQueryOnUnknownIdIsNoOp(
            @ForAll @AlphaChars @StringLength(min = 1, max = 30) String queryId) {
        CountingConnectorMetadata counting = new CountingConnectorMetadata();
        CatalogMetadata service = buildService(counting);
        var query = new org.pangrova.coordinator.task.TestConnectorQuery(queryId);

        query.seal();
        service.cleanupQuery(query.authority());

        assertThat(counting.cleanupCount.get())
                .as("cleanupQuery on unknown queryId should not delegate")
                .isEqualTo(0);
    }

    /**
     * Property 7: begin then cleanup then begin again works correctly.
     */
    @Property(tries = 10)
    void queryCanBeRestarted(
            @ForAll @AlphaChars @StringLength(min = 1, max = 30) String queryId) {
        CountingConnectorMetadata counting = new CountingConnectorMetadata();
        CatalogMetadata service = buildService(counting);
        var query = new org.pangrova.coordinator.task.TestConnectorQuery(queryId);

        service.beginQuery(query.metadataSession());
        query.seal();
        service.cleanupQuery(query.authority());
        query.retire();
        assertThat(query.authority().isClosed()).isTrue();
        query = new org.pangrova.coordinator.task.TestConnectorQuery(queryId);
        service.beginQuery(query.metadataSession());
        query.seal();
        service.cleanupQuery(query.authority());

        assertThat(counting.beginCount.get()).isEqualTo(2);
        assertThat(counting.cleanupCount.get()).isEqualTo(2);
    }

    // ── Test infrastructure ──

    private CatalogMetadata buildService(CountingConnectorMetadata metadata) {
        EmptyCatalogDeployments store = new EmptyCatalogDeployments();
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new StubConnector(metadata));
        CatalogRegistry registry = new CatalogRegistry(memoryTableStores(), store, chf, null, null);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                "test", CatalogType.CONNECTOR, "stub",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        registry.register(descriptor);
        registry.initialize();

        return new CatalogMetadata(
                registry,
                new TieredMetadataCache(),
                new MetadataEventDispatcher(),
                new SchemaEvolutionValidator());
    }

    /**
     * ConnectorMetadata that counts beginQuery/cleanupQuery invocations.
     */
    static final class CountingConnectorMetadata implements ConnectorMetadata {
        final AtomicInteger beginCount = new AtomicInteger();
        final AtomicInteger cleanupCount = new AtomicInteger();

        @Override
        public void beginQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId();
            beginCount.incrementAndGet();
        }

        @Override
        public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId();
            cleanupCount.incrementAndGet();
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle h) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle h) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle h) {
            return TableStatistics.UNKNOWN;
        }
    }

    /**
     * Minimal Connector that returns the given ConnectorMetadata.
     */
    static final class StubConnector implements Connector {
        private final ConnectorMetadata metadata;

        StubConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() { }
    }

    /**
     * Minimal CatalogDeploymentRepository for testing.
     */
    static final class EmptyCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return catalogs.stream().filter(d -> d.name().equals(n)).findFirst();
        }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { return catalogs.removeIf(d -> d.identity().equals(expected)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }

    }
}
