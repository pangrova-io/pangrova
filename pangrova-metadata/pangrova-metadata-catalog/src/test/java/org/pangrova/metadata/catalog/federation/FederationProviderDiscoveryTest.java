package org.pangrova.metadata.catalog.federation;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.Optional;
import java.time.Instant;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.MetadataChangeBatch;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.cache.TieredMetadataCache;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.metadata.api.catalog.FederatedCatalog;
import org.pangrova.metadata.api.catalog.FederatedCatalogFactory;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FederationProviderDiscoveryTest {
    @Test
    void duplicateIdsFailDuringDiscoveryBeforeAnyCatalogIsOpened(@TempDir Path root) throws Exception {
        Path descriptor = root.resolve("META-INF/services/" + FederatedCatalogFactory.class.getName());
        Files.createDirectories(descriptor.getParent());
        Files.writeString(descriptor, First.class.getName() + "\n" + Second.class.getName() + "\n");
        try (var loader = new URLClassLoader(new URL[]{root.toUri().toURL()}, getClass().getClassLoader())) {
            assertThatThrownBy(() -> new PluginFederationHandleFactory(loader, ref -> ref.key(), new TieredMetadataCache()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Duplicate federated catalog provider 'warehouse'")
                    .hasMessageContaining(First.class.getName()).hasMessageContaining(Second.class.getName());
        }
    }

    @Test
    void readsShareTheCacheButSynchronizationAndResourceClosureUseTheOriginalSource(@TempDir Path root)
            throws Exception {
        Path descriptor = root.resolve("META-INF/services/" + FederatedCatalogFactory.class.getName());
        Files.createDirectories(descriptor.getParent());
        Files.writeString(descriptor, CountingFactory.class.getName());
        try (var loader = new URLClassLoader(new URL[]{root.toUri().toURL()}, getClass().getClassLoader());
             var cache = new TieredMetadataCache()) {
            var factory = new PluginFederationHandleFactory(loader, ref -> ref.key(), cache);
            var handle = factory.create(new CatalogDescriptor("catalog", CatalogType.FEDERATION, "counting",
                    CatalogState.ACTIVE, Map.of(), "", Set.of()));
            CountingCatalog source = (CountingCatalog) handle.federationSource().orElseThrow();
            try {
                var read = handle.getMetadata(null);
                var table = read.getTableHandle(null, new SchemaTableName("schema", "table"));
                read.getTableMetadata(null, table);
                assertThat(source.reads).isOne();
                source.getTable("schema", "table");
                assertThat(source.reads).isEqualTo(2);
                cache.invalidateTable("catalog", "schema", "table");
                read.getTableHandle(null, new SchemaTableName("schema", "table"));
                assertThat(source.reads).isEqualTo(3);
            } finally { handle.shutdown(); }
            assertThat(source.closes).isOne();
        }
    }

    public static final class CountingFactory implements FederatedCatalogFactory {
        @Override public String name() { return "counting"; }
        @Override public FederatedCatalog create(Map<String, String> properties) { return new CountingCatalog(); }
    }

    private static final class CountingCatalog implements FederatedCatalog {
        private int reads;
        private int closes;
        @Override public List<String> listSchemas() { return List.of("schema"); }
        @Override public List<String> listTables(String schema) { return List.of("table"); }
        @Override public TableMetadata getTable(String schema, String table) {
            reads++;
            return new TableMetadata("catalog", schema, table, List.of(), Map.of(), List.of(), Map.of(), null, null);
        }
        @Override public TableStatistics getTableStatistics(String schema, String table) { return TableStatistics.UNKNOWN; }
        @Override public MetadataChangeBatch readChangesSince(Instant since) { return new MetadataChangeBatch(List.of(), Optional.empty()); }
        @Override public void close() { closes++; }
    }

    public static class First implements FederatedCatalogFactory {
        @Override
        public String name() {
            return "warehouse";
        }

        @Override
        public FederatedCatalog create(Map<String, String> properties) {
            throw new AssertionError("Discovery opened a catalog before validating the factory inventory");
        }
    }

    public static class Second extends First {}
}
