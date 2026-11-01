package org.pangrova.coordinator.metadata;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.store.jdbc.catalog.JdbcMetadataStore;
import org.pangrova.security.secret.SecretResolverRegistry;
import org.pangrova.spi.connector.plugin.ConnectorFactory;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.security.UserIdentity;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Actual migrated PG directory, production plugin loader and physical SDK commits. */
public final class ReadBindingSdkFixture implements AutoCloseable {
    public final Catalog sdk;
    public final MetadataBootstrap bootstrap;
    public final CoordinatorSessionContext context = new CoordinatorSessionContext("bound-query",
            Map.of("catalog", "bound", "schema", "db"),
            new UserIdentity("reader", Set.of(), Map.of(), Instant.EPOCH), "tenant");
    public final ConnectorSession session = SessionContextConverter.toConnectorSession(context);
    private final PluginConnectorHandleFactory factory;

    public ReadBindingSdkFixture(Path warehouse) throws Exception {
        var options = new Options();
        options.setString("warehouse", warehouse.toString());
        sdk = CatalogFactory.createCatalog(CatalogContext.create(options));
        factory = new PluginConnectorHandleFactory(ConnectorFactory.class.getClassLoader(),
                new SecretResolverRegistry(), ignored -> Optional.empty());
        try {
            sdk.createDatabase("db", false);
            var store = new JdbcMetadataStore(SharedPostgresContainer.migratedDataSource("read_binding"));
            store.saveCatalogDescriptor(new CatalogDescriptor("bound", CatalogType.CONNECTOR, "paimon",
                    CatalogState.ACTIVE, Map.of("warehouse", warehouse.toString()), "", Set.of("tenant")));
            bootstrap = MetadataBootstrap.initialize(store, factory);
        } catch (Exception | Error failure) {
            try { factory.shutdown(); } catch (Throwable error) { failure.addSuppressed(error); }
            try { sdk.close(); } catch (Throwable error) { failure.addSuppressed(error); }
            throw failure;
        }
    }

    public FileStoreTable create(String name) throws Exception {
        return create("db", name);
    }

    /** Creates an actual SDK table with separate resolved namespace components. */
    public FileStoreTable create(String schema, String name) throws Exception {
        var id = Identifier.create(schema, name);
        sdk.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT())
                .option("bucket", "-1").option("file.format", "parquet").build(), false);
        return (FileStoreTable) sdk.getTable(id);
    }

    public static void write(FileStoreTable table, int rows) throws Exception {
        var builder = table.newBatchWriteBuilder();
        try (var writer = builder.newWrite(); var commit = builder.newCommit()) {
            for (int row = 0; row < rows; row++) writer.write(GenericRow.of((long) row));
            commit.commit(writer.prepareCommit());
        }
    }

    public List<org.pangrova.spi.connector.metadata.ConnectorSplit> splits(
            org.pangrova.spi.connector.metadata.table.ConnectorReadBinding binding,
            Map<String, Long> overrides) {
        var descriptor = new org.pangrova.planner.SplitSourceDescriptor("bound", "db",
                binding.declaration().metadata().table().table(), org.pangrova.spi.connector.metadata.Constraint.EMPTY,
                binding.declaration().metadata().columns(), Optional.of(binding));
        var tracker = new org.pangrova.coordinator.task.CoordinatorTaskTracker();
        var state = new org.pangrova.coordinator.query.QueryStateMachine();
        var authority = state.bindConnectorAuthority(tracker.openQueryIssuance(context.queryId()));
        var admitted = context.withQueryAuthority(authority);
        var actor = SessionContextConverter.toConnectorSession(admitted, "bound", "db");
        try {
            authority.beginMetadata("bound", () -> bootstrap.catalogRegistry().getCatalog("bound").getMetadata(actor));
            return org.pangrova.coordinator.query.execution.split.SplitFetcher.fetchRawConnectorSplits(
                    List.of(descriptor), bootstrap.catalogRegistry(), overrides,
                    binding.declaration().metadata().columns(), admitted);
        } finally {
            state.tryTransition(org.pangrova.coordinator.query.QueryStateMachine.State.CANCELLED);
            tracker.sealQueryIssuance(context.queryId());
            authority.cleanupMetadata().requireSuccess();
            tracker.retireQueryIssuance(context.queryId());
        }
    }

    @Override public void close() throws Exception {
        Throwable failure = null;
        try { bootstrap.shutdown(); } catch (Throwable error) { failure = error; }
        try { factory.shutdown(); } catch (Throwable error) {
            if (failure == null) failure = error; else failure.addSuppressed(error);
        }
        try { sdk.close(); } catch (Throwable error) {
            if (failure == null) failure = error; else failure.addSuppressed(error);
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof Exception error) throw error;
    }
}
