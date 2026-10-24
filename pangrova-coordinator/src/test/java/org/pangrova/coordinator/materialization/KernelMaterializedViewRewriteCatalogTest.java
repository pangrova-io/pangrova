package org.pangrova.coordinator.materialization;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pangrova.materialization.domain.ColumnDefinition;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.OutputSchema;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.QualifiedName;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.MaterializationCatalog;
import org.pangrova.materialization.port.PublishedMaterialization;
import org.pangrova.planner.mv.MaterializedViewRewriteCatalog;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.AccessDeniedException;

class KernelMaterializedViewRewriteCatalogTest {

    private static final String TENANT = "tenant-a";
    private static final Instant NOW = Instant.parse("2026-10-23T00:00:00Z");
    private static final SourceRef ORDERS = new SourceRef("iceberg", "lake", "db", "orders");

    @Test
    void exposesPublishedViewWithItsPublishedGeneration() {
        var catalog = new FakeCatalog(List.of(materializedView("lake.db.orders_mv", NOW)));
        var rewrite = create(catalog, session("alice", Map.of()));

        assertThat(rewrite).isPresent();
        var views = rewrite.orElseThrow().candidates(Set.of("lake.db.orders"));
        assertThat(views).singleElement().satisfies(view -> {
            assertThat(view.qualifiedName()).isEqualTo("lake.db.orders_mv");
            assertThat(view.definitionSql()).isEqualTo("SELECT id, SUM(amount) AS total FROM orders GROUP BY id");
            assertThat(view.physicalSchema()).isEqualTo("__pangrova_materializations");
            assertThat(view.maxStaleness()).isEqualTo(Duration.ofMinutes(30));
        });
    }

    @Test
    void rewriteIsDisabledBySessionProperty() {
        var catalog = new FakeCatalog(List.of(materializedView("lake.db.orders_mv", NOW)));

        assertThat(create(catalog, session("alice", Map.of(
                KernelMaterializedViewRewriteCatalog.ENABLED_PROPERTY, "off")))).isEmpty();
    }

    @Test
    void invalidStalenessDisablesRewriteInsteadOfExposingEverything() {
        var catalog = new FakeCatalog(List.of(materializedView("lake.db.orders_mv", NOW)));

        assertThat(create(catalog, session("alice", Map.of(
                KernelMaterializedViewRewriteCatalog.MAX_STALENESS_PROPERTY, "soon")))).isEmpty();
        assertThat(create(catalog, session("alice", Map.of(
                KernelMaterializedViewRewriteCatalog.MAX_STALENESS_PROPERTY, "0")))).isEmpty();
    }

    @Test
    void dynamicTablesAreNeverRewriteCandidates() {
        var dynamicTable = materialization("lake.db.orders_dt", NOW,
                MaterializationKind.DYNAMIC_TABLE);
        var catalog = new FakeCatalog(List.of(dynamicTable));

        var rewrite = create(catalog, session("alice", Map.of()));

        assertThat(rewrite).isPresent();
        assertThat(rewrite.orElseThrow().candidates(Set.of("lake.db.orders"))).isEmpty();
    }

    @Test
    void staleGenerationIsNotOffered() {
        var catalog = new FakeCatalog(List.of(
                materializedView("lake.db.orders_mv", NOW.minus(Duration.ofHours(2)))));
        var rewrite = create(catalog, session("alice", Map.of())).orElseThrow();

        assertThat(rewrite.candidates(Set.of("lake.db.orders"))).isEmpty();
    }

    @Test
    void viewsInAnotherSchemaAreNotOffered() {
        var catalog = new FakeCatalog(List.of(materializedView("lake.other.orders_mv", NOW)));
        var rewrite = create(catalog, session("alice", Map.of())).orElseThrow();

        assertThat(rewrite.candidates(Set.of("lake.db.orders"))).isEmpty();
    }

    @Test
    void deniedMetadataReadHidesTheView() {
        var catalog = new FakeCatalog(List.of(materializedView("lake.db.orders_mv", NOW)));
        var rewrite = create(catalog, session("alice", Map.of()), denyMetadata()).orElseThrow();

        assertThat(rewrite.candidates(Set.of("lake.db.orders"))).isEmpty();
    }

    @Test
    void anonymousSessionCannotRewrite() {
        var catalog = new FakeCatalog(List.of(materializedView("lake.db.orders_mv", NOW)));

        assertThat(create(catalog, session("anonymous", Map.of()))).isEmpty();
    }

    private Optional<MaterializedViewRewriteCatalog> create(MaterializationCatalog catalog,
            ConnectorSession session) {
        return create(catalog, session, allowAll());
    }

    private Optional<MaterializedViewRewriteCatalog> create(MaterializationCatalog catalog,
            ConnectorSession session, AccessControl accessControl) {
        return KernelMaterializedViewRewriteCatalog.create(catalog, TENANT, session,
                accessControl, NOW);
    }

    private static ConnectorSession session(String user, Map<String, String> properties) {
        return new ConnectorSession() {
            @Override
            public String getUser() {
                return user;
            }

            @Override
            public Optional<String> getCatalog() {
                return Optional.of("lake");
            }

            @Override
            public Optional<String> getSchema() {
                return Optional.of("db");
            }

            @Override
            public Map<String, String> getProperties() {
                return properties;
            }

            @Override
            public Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
                return Optional.empty();
            }
        };
    }

    private static PublishedMaterialization materializedView(String name, Instant publishedAt) {
        return materialization(name, publishedAt, MaterializationKind.MATERIALIZED_VIEW);
    }

    private static PublishedMaterialization materialization(String name, Instant publishedAt,
            MaterializationKind kind) {
        MaterializationId object = new MaterializationId(new TenantId(TENANT),
                QualifiedName.parse(name));
        return new PublishedMaterialization(object, kind, new GenerationId(UUID.randomUUID()),
                new PhysicalTableRef("lake", "__pangrova_materializations",
                        "__pg_mat_" + "a".repeat(32)),
                1, "SELECT id, SUM(amount) AS total FROM orders GROUP BY id",
                new OutputSchema(List.of(
                        new ColumnDefinition("id", "BIGINT", false, 0),
                        new ColumnDefinition("total", "BIGINT", true, 1))),
                Set.of(ORDERS), RefreshKind.Full.INSTANCE,
                new SourceVector(Map.of(), 1), publishedAt);
    }

    private static AccessControl allowAll() {
        return accessControl(false);
    }

    private static AccessControl denyMetadata() {
        return accessControl(true);
    }

    private static AccessControl accessControl(boolean deny) {
        return (AccessControl) Proxy.newProxyInstance(AccessControl.class.getClassLoader(),
                new Class<?>[] {AccessControl.class}, (proxy, method, args) -> {
                    if (deny && method.getName().equals("checkCanReadTableMetadata")) {
                        throw new AccessDeniedException("denied by test policy");
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
    }

    private static final class FakeCatalog implements MaterializationCatalog {

        private final List<PublishedMaterialization> published;

        private FakeCatalog(List<PublishedMaterialization> published) {
            this.published = published;
        }

        @Override
        public Optional<PublishedMaterialization> findPublished(MaterializationId object) {
            return published.stream()
                    .filter(candidate -> candidate.object().equals(object))
                    .findFirst();
        }

        @Override
        public List<PublishedMaterialization> listPublished(TenantId tenant) {
            return published.stream()
                    .filter(candidate -> candidate.object().tenant().equals(tenant))
                    .toList();
        }

        @Override
        public long registryEpoch(TenantId tenant) {
            return 1;
        }
    }
}
