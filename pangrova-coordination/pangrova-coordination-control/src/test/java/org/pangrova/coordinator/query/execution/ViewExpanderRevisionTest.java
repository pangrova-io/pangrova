package org.pangrova.coordinator.query.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.enums.ViewCreateMode;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.iceberg.IcebergConnectorFactory;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.task.TestConnectorQuery;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;
import org.pangrova.parser.syntax.TimeTravelSqlClauses;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.query.api.session.QuerySession;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

/** Actual query lifetime and Iceberg view definitions must agree on a single catalog revision. */
class ViewExpanderRevisionTest {
    @TempDir Path warehouse;

    @Test
    void expandsTheQueryRetainedViewRevisionRatherThanTheCurrentDeployment() {
        var dispatcher = new MetadataEventDispatcher();
        var factory = new IcebergConnectorFactory();
        var registry = new CatalogRegistry(memoryTableStores(), new DeploymentFixture(),
                descriptor -> new ConnectorCatalogHandle(descriptor,
                        factory.create(descriptor.name(), descriptor.properties(), null)),
                null, dispatcher);
        var oldQuery = new TestConnectorQuery("old-view-reader");
        var newQuery = new TestConnectorQuery("new-view-reader");
        try {
            registry.register(new CatalogDescriptor("lake", CatalogType.CONNECTOR, "iceberg", CatalogState.ACTIVE,
                    Map.of("type", "hadoop", "warehouse", warehouse.resolve("first").toString()), "", Set.of()));
            registry.initialize();
            var oldSession = context(oldQuery);
            var oldActor = SessionContextConverter.toConnectorSession(oldSession, "lake", "raw");
            var view = new SchemaTableName("raw", "report");
            var first = registry.queryCatalog("lake", oldActor);
            first.metadata().createView(oldActor, view, "SELECT 11 AS id", ViewCreateMode.CREATE);

            registry.alterProperties("lake", null, Map.of("warehouse", warehouse.resolve("second").toString()));
            var newSession = context(newQuery);
            var newActor = SessionContextConverter.toConnectorSession(newSession, "lake", "raw");
            var second = registry.queryCatalog("lake", newActor);
            assertThat(second.handle().getDescriptor().revision()).isEqualTo(2);
            assertThat(first.handle()).isNotSameAs(second.handle());
            second.metadata().createView(newActor, view, "SELECT 22 AS id", ViewCreateMode.CREATE);

            var input = TimeTravelSqlClauses.extract("SELECT report.id FROM lake.raw.report");
            var expander = new ViewExpander(registry);
            assertThat(expander.expand(input, oldSession).sql()).contains("SELECT 11 AS id").doesNotContain("22");
            assertThat(expander.expand(input, newSession).sql()).contains("SELECT 22 AS id").doesNotContain("11");
        } finally {
            cleanup(oldQuery);
            cleanup(newQuery);
            registry.shutdown();
            dispatcher.shutdown();
        }
    }

    private static QuerySession context(TestConnectorQuery query) {
        return new QuerySession(query.authority().queryId(), Map.of("catalog", "lake", "schema", "raw"),
                null, "component-tenant", Optional.of(query.authority()),
                new OtelExecutionTraceContextFactory().capture());
    }

    private static void cleanup(TestConnectorQuery query) {
        query.seal();
        query.authority().cleanupMetadata().requireSuccess();
        query.retire();
    }

    private static final class DeploymentFixture implements CatalogDeploymentRepository {
        private final Map<String, CatalogDescriptor> current = new LinkedHashMap<>();

        @Override public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
            current.put(descriptor.name(), descriptor);
        }

        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return Optional.ofNullable(current.get(name));
        }

        @Override public Optional<CatalogDescriptor> findCatalogDescriptorRevision(
                String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) {
            return current.values().removeIf(descriptor -> descriptor.identity().equals(expected));
        }

        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.copyOf(current.values());
        }
    }
}
