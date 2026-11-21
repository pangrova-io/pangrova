package org.pangrova.coordinator.indexing.ddl;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.AlterIndexStatement;
import org.pangrova.parser.api.ast.statement.CreateIndexStatement;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.types.enums.PangrovaType;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexDdlReadAuthorizationTest {

    private static final Identifier TABLE = Identifier.of("cat", "sch", "tbl");
    private static final UserIdentity USER = new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH);
    private static final QuerySession SESSION =
            new QuerySession("q-index", Map.of(), USER, "tenant-a", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

    private IndexDdlExecutor executor;
    private IndexBuildCoordinator builder;

    @AfterEach
    void close() {
        if (executor != null) {
            executor.close();
        }
        if (builder != null) {
            builder.close();
        }
    }

    @Test
    void createRequiresSelectOnTheExactSourceColumnsBeforeSubmittingBuild() {
        var checkedColumns = new AtomicReference<Set<String>>();
        executor = executor(accessControl(checkedColumns, true, false, false), metadataStore(null));

        assertThatThrownBy(() -> executor.executeCreateIndex(create(), SESSION))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("source SELECT denied");
        assertThat(checkedColumns.get()).containsExactly("col1");
    }

    @Test
    void createRejectsRawReadWhenTheSourceHasARowFilter() {
        executor = executor(accessControl(new AtomicReference<>(), false, true, false), metadataStore(null));

        assertThatThrownBy(() -> executor.executeCreateIndex(create(), SESSION))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("row filter");
    }

    @Test
    void createRejectsRawReadWhenAnIndexedColumnIsMasked() {
        executor = executor(accessControl(new AtomicReference<>(), false, false, true), metadataStore(null));

        assertThatThrownBy(() -> executor.executeCreateIndex(create(), SESSION))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("column mask");
    }

    @Test
    void createChecksTheCanonicalColumnNameWhenTheSqlSpellingDiffers() {
        var checkedColumns = new AtomicReference<Set<String>>();
        executor = executor(accessControl(checkedColumns, false, false, true), metadataStore(null));
        var statement = new CreateIndexStatement("idx", TABLE, List.of("COL1"),
                "BLOOM_FILTER", false, Map.of());

        assertThatThrownBy(() -> executor.executeCreateIndex(statement, SESSION))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("column mask");
        assertThat(checkedColumns.get()).containsExactly("col1");
    }

    @Test
    void rebuildChecksThePersistedIndexColumnsBeforeReadingSourceFiles() {
        var checkedColumns = new AtomicReference<Set<String>>();
        executor = executor(accessControl(checkedColumns, true, false, false), metadataStore(index()));

        assertThatThrownBy(() -> executor.executeAlterIndex(new AlterIndexStatement("idx", TABLE), SESSION))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("source SELECT denied");
        assertThat(checkedColumns.get()).containsExactly("col1");
    }

    private IndexDdlExecutor executor(AccessControl accessControl, IndexMetadataStore store) {
        builder = new IndexBuildCoordinator(new IndexBuildContext(
                new IndexingConfig("index-ddl-authorization", false, null, null),
                null, null, null), null);
        var registry = catalogRegistry();
        var dependencies = new IndexDdlContext(store, new IndexBuildServices(builder, null),
                registry, new ConnectorAccessorResolver(registry));
        return new IndexDdlExecutor(dependencies, null, accessControl);
    }

    private static CreateIndexStatement create() {
        return new CreateIndexStatement("idx", TABLE, List.of("col1"),
                "BLOOM_FILTER", false, Map.of());
    }

    private static IndexDescriptor index() {
        return new IndexDescriptor("index-1", "idx", "cat", "sch", "tbl", List.of("col1"),
                IndexType.BLOOM_FILTER, Map.of(), IndexState.READY, null, 0,
                "snap-1", "snap-1", Instant.EPOCH, Instant.EPOCH, "tenant-a");
    }

    private static IndexMetadataStore metadataStore(IndexDescriptor descriptor) {
        return (IndexMetadataStore) Proxy.newProxyInstance(
                IndexMetadataStore.class.getClassLoader(), new Class<?>[] {IndexMetadataStore.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("findIndexesByTable")) {
                        return descriptor == null ? List.of() : List.of(descriptor);
                    }
                    throw new AssertionError("Unexpected metadata access: " + method.getName());
                });
    }

    private static CatalogRegistry catalogRegistry() {
        var table = new ConnectorTableMetadata(new SchemaTableName("sch", "tbl"),
                List.of("col1"), List.of(new ConnectorTableMetadata.Column("col1", PangrovaType.VARCHAR)),
                Map.of());
        ConnectorTableHandle tableHandle = new ConnectorTableHandle() {};
        var metadata = (ConnectorMetadata) Proxy.newProxyInstance(
                ConnectorMetadata.class.getClassLoader(), new Class<?>[] {ConnectorMetadata.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getTableHandle" -> tableHandle;
                    case "getTableMetadata" -> table;
                    default -> throw new AssertionError("Unexpected connector metadata access: " + method.getName());
                });
        var connector = (Connector) Proxy.newProxyInstance(
                Connector.class.getClassLoader(), new Class<?>[] {Connector.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("getMetadata")) {
                        return metadata;
                    }
                    throw new AssertionError("Unexpected connector access: " + method.getName());
                });
        var descriptor = new CatalogDescriptor("cat", CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        CatalogHandle handle = new ConnectorCatalogHandle(descriptor, connector);
        return new CatalogRegistry(memoryTableStores(), null) {
            @Override
            public CatalogHandle getCatalog(String name) {
                assertThat(name).isEqualTo("cat");
                return handle;
            }
        };
    }

    private static AccessControl accessControl(AtomicReference<Set<String>> checkedColumns,
                                               boolean denySelect, boolean rowFilter, boolean columnMask) {
        return (AccessControl) Proxy.newProxyInstance(
                AccessControl.class.getClassLoader(), new Class<?>[] {AccessControl.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "checkCanAlterTable" -> null;
                    case "checkCanSelectFromColumns" -> {
                        @SuppressWarnings("unchecked")
                        Set<String> columns = (Set<String>) arguments[4];
                        checkedColumns.set(columns);
                        if (denySelect) {
                            throw new AccessDeniedException("source SELECT denied");
                        }
                        yield null;
                    }
                    case "getRowFilter" -> rowFilter ? Optional.of("region = 'west'") : Optional.empty();
                    case "getColumnMask" -> columnMask && "col1".equals(arguments[4])
                            ? Optional.of("NULL") : Optional.empty();
                    default -> throw new AssertionError("Unexpected access check: " + method.getName());
                });
    }
}
