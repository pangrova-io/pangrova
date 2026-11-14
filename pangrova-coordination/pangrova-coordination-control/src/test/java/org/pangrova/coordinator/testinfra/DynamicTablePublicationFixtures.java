package org.pangrova.coordinator.testinfra;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.pangrova.materialization.api.domain.ColumnDefinition;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.domain.InternalNamespace;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.domain.OutputSchema;
import org.pangrova.materialization.api.domain.PhysicalTableRef;
import org.pangrova.materialization.api.domain.QualifiedName;
import org.pangrova.materialization.api.enums.RefreshKind;
import org.pangrova.materialization.api.domain.SourceVector;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.persistence.materialization.api.publication.MaterializationCatalog;
import org.pangrova.materialization.api.port.PublishedMaterialization;

/**
 * Component-safe published read model for resolver and SQL-security tests.
 *
 * <p>The in-memory catalog mirrors {@code MaterializationCatalog} semantics so
 * tests never depend on the legacy publication store. Durable publication is
 * covered by the metadata repository tests against the real V68 schema.
 */
public final class DynamicTablePublicationFixtures {

    private DynamicTablePublicationFixtures() {
    }

    public static InMemoryCatalog catalog() {
        return new InMemoryCatalog();
    }

    public static PublishedMaterialization create(InMemoryCatalog catalog, String tenant,
            String logicalName, String definitionSql) {
        QualifiedName name = QualifiedName.parse(logicalName);
        MaterializationId object = new MaterializationId(new TenantId(tenant), name);
        PublishedMaterialization published = projection(object, 1, definitionSql);
        catalog.put(published);
        return published;
    }

    public static PublishedMaterialization refresh(InMemoryCatalog catalog,
            PublishedMaterialization current) {
        PublishedMaterialization next = projection(current.object(),
                current.definitionRevision() + 1, current.definitionSql());
        catalog.put(next);
        return next;
    }

    private static PublishedMaterialization projection(MaterializationId object, long revision,
            String definitionSql) {
        String hex = UUID.randomUUID().toString().replace("-", "");
        PhysicalTableRef physical = new PhysicalTableRef(
                object.name().parts().get(0), InternalNamespace.SCHEMA_NAME,
                InternalNamespace.candidateTableName(hex));
        return new PublishedMaterialization(
                object,
                MaterializationKind.DYNAMIC_TABLE,
                new GenerationId(UUID.randomUUID()),
                physical,
                revision,
                definitionSql,
                new OutputSchema(List.of(
                        new ColumnDefinition("id", "BIGINT", false, 0),
                        new ColumnDefinition("region", "VARCHAR", true, 1),
                        new ColumnDefinition("department", "VARCHAR", true, 2))),
                java.util.Set.of(),
                RefreshKind.FULL,
                new SourceVector(Map.of(), revision),
                Instant.now());
    }

    /** Thread-safe catalog that advances the registry epoch on every head mutation. */
    public static final class InMemoryCatalog implements MaterializationCatalog {

        private final Map<MaterializationId, PublishedMaterialization> published =
                new ConcurrentHashMap<>();
        private final Map<TenantId, AtomicLong> epochs = new ConcurrentHashMap<>();

        private void put(PublishedMaterialization materialization) {
            published.put(materialization.object(), materialization);
            epochs.computeIfAbsent(materialization.object().tenant(), tenant -> new AtomicLong())
                    .incrementAndGet();
        }

        @Override
        public Optional<PublishedMaterialization> findPublished(MaterializationId object) {
            return Optional.ofNullable(published.get(object));
        }

        @Override
        public List<PublishedMaterialization> listPublished(TenantId tenant) {
            return published.values().stream()
                    .filter(materialization -> materialization.object().tenant().equals(tenant))
                    .sorted(Comparator.comparing(materialization ->
                            materialization.object().name().toString()))
                    .toList();
        }

        @Override
        public long registryEpoch(TenantId tenant) {
            AtomicLong epoch = epochs.get(tenant);
            return epoch == null ? 0L : epoch.get();
        }
    }
}
