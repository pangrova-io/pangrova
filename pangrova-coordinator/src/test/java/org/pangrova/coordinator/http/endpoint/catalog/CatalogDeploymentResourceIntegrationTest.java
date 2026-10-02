package org.pangrova.coordinator.http.endpoint.catalog;

import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.WebApplicationException;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.metadata.store.JdbcMetadataStore;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogDeploymentResourceIntegrationTest {

    @Test
    void servesOnlyExactHistoricalSnapshotAfterRevisionAndIncarnationChange() throws Exception {
        var store = new JdbcMetadataStore(
                SharedPostgresContainer.migratedDataSource("catalog_deployment_history"));
        var endpoint = new CatalogDeploymentResource(
                new DefaultMetadataService(new CatalogRegistry(store), null, null, null),
                "internal-test-secret");
        CatalogDescriptor old = new CatalogDescriptor("catalog", CatalogType.INTERNAL, "iceberg",
                CatalogState.ACTIVE, Map.of("path", "old"), "", Set.of("tenant-a"));
        store.saveCatalogDescriptor(old);
        CatalogDescriptor newer = new CatalogDescriptor(old.name(), old.type(), old.provider(),
                old.state(), Map.of("path", "new"), "", Set.of("tenant-b"),
                old.incarnationId(), 2);
        store.saveCatalogDescriptor(newer);

        var historical = endpoint.get("catalog", old.incarnationId(), 1, "internal-test-secret");
        assertThat(historical.properties()).containsEntry("path", "old");
        assertThat(historical.allowedTenants()).containsExactly("tenant-a");
        assertThat(historical.catalogRevision()).isEqualTo(1);
        assertThat(endpoint.get("catalog", old.incarnationId(), 2, "internal-test-secret")
                .properties()).containsEntry("path", "new");

        store.deleteCatalogDescriptor("catalog");
        CatalogDescriptor replacement = new CatalogDescriptor("catalog", CatalogType.INTERNAL,
                "iceberg", CatalogState.ACTIVE, Map.of("path", "replacement"), "", Set.of());
        store.saveCatalogDescriptor(replacement);
        assertThat(endpoint.get("catalog", old.incarnationId(), 1, "internal-test-secret")
                .properties()).containsEntry("path", "old");
        assertThatThrownBy(() -> endpoint.get("catalog", replacement.incarnationId(), 2,
                "internal-test-secret")).isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> endpoint.get("catalog", "missing", 1,
                "internal-test-secret")).isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> endpoint.get("catalog", old.incarnationId(), 0,
                "internal-test-secret")).isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> endpoint.get("catalog", old.incarnationId(), 1,
                "incorrect-secret")).isInstanceOf(WebApplicationException.class)
                .satisfies(error -> assertThat(((WebApplicationException) error)
                        .getResponse().getStatus()).isEqualTo(401));
    }
}
