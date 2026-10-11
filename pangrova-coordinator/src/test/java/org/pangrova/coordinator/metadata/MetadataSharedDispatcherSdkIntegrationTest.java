package org.pangrova.coordinator.metadata;

import org.apache.paimon.catalog.Identifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.model.ColumnMetadata;
import org.pangrova.metadata.model.ColumnType;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.metadata.service.MetadataSessionContext;
import org.pangrova.testing.ContainerTestBase;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Actual PG/plugin/SDK DDL proves borrowed dispatcher identity, not production bootstrap or Native E2E. */
class MetadataSharedDispatcherSdkIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void defaultServiceDispatchesThroughTheProvidedActualBusAfterRealSdkDrop() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse);
                var dispatcher = new MetadataEventDispatcher()) {
            var physical = fixture.create("dispatcher_owned");
            var columns = physical.rowType().getFields().stream().map(field ->
                    new ColumnMetadata(field.name(), ColumnType.of(field.type().getTypeRoot().name()),
                            field.type().isNullable(), "", Map.of())).toList();
            var metadata = new TableMetadata("bound", "db", "dispatcher_owned", columns,
                    Map.of(), List.of(), Map.of(), null, null);
            var primary = fixture.bootstrap.metadataCache();
            var observer = new TieredMetadataCache();
            dispatcher.addListener(primary);
            dispatcher.addListener(observer);
            var service = new DefaultMetadataService(fixture.bootstrap.catalogRegistry(), primary, dispatcher,
                    new SchemaEvolutionValidator());
            var session = new MetadataSessionContext("reader", "bound", "db", "UTC", "en", Map.of(),
                    "tenant", "", fixture.context.queryId());
            primary.putTable("bound", "db", "dispatcher_owned", metadata);
            observer.putTable("bound", "db", "dispatcher_owned", metadata);
            assertThat(observer.getTable("bound", "db", "dispatcher_owned")).contains(metadata);

            service.dropTable(session, new QualifiedTableRef("bound", "db", "dispatcher_owned"));

            // The fixture's separate SDK cache may retain a table object; reopen the actual persisted catalog.
            var options = new org.apache.paimon.options.Options();
            options.setString("warehouse", warehouse.toString());
            try (var fresh = org.apache.paimon.catalog.CatalogFactory.createCatalog(
                    org.apache.paimon.catalog.CatalogContext.create(options))) {
                assertThatThrownBy(() -> fresh.getTable(Identifier.create("db", "dispatcher_owned")))
                        .isInstanceOf(org.apache.paimon.catalog.Catalog.TableNotExistException.class);
            }
            assertThat(primary.getTable("bound", "db", "dispatcher_owned")).isEmpty();
            // Only the provided dispatcher can reach this real observer cache; direct invalidation cannot.
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(observer.getTable("bound", "db", "dispatcher_owned")).isEmpty());
        }
    }
}
