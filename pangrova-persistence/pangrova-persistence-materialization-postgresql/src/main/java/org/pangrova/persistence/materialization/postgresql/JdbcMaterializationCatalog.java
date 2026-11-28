package org.pangrova.persistence.materialization.postgresql;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.enums.MaterializationKind;
import org.pangrova.materialization.api.domain.OutputSchema;
import org.pangrova.materialization.api.domain.PhysicalTableRef;
import org.pangrova.materialization.api.domain.QualifiedName;
import org.pangrova.materialization.api.enums.RefreshKind;
import org.pangrova.materialization.api.domain.SourceVector;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.persistence.materialization.api.publication.MaterializationCatalog;
import org.pangrova.persistence.materialization.api.publication.MaterializationPublicationSnapshot;
import org.pangrova.materialization.api.port.PublishedMaterialization;
import org.pangrova.connector.api.materialization.SourceRef;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;

/**
 * Durable read model of published materializations.
 *
 * <p>The projection is always derived from the authoritative header, the
 * published generation, and its immutable definition revision. Callers must
 * include {@link #registryEpoch(TenantId)} in plan and result cache keys; the
 * V68 object triggers advance the epoch on every head mutation.
 */
@org.pangrova.common.concurrent.ThreadSafe
public final class JdbcMaterializationCatalog implements MaterializationCatalog {

    private static final String PUBLISHED_SELECT =
            "SELECT o.tenant_id, o.object_name, o.object_kind, o.published_generation_id, "
                    + "g.definition_revision, g.physical_catalog, g.physical_schema, "
                    + "g.physical_table, g.refresh_kind, g.source_vector, g.published_at, "
                    + "d.definition_sql, d.output_schema, d.sources, d.resolved "
                    + "FROM materialization.objects o "
                    + "JOIN materialization.generations g "
                    + "ON g.generation_id=o.published_generation_id "
                    + "JOIN materialization.definition_revisions d "
                    + "ON d.tenant_id=o.tenant_id AND d.object_name=o.object_name "
                    + "AND d.revision=g.definition_revision "
                    + "WHERE o.state='ACTIVE' AND d.resolved";

    private static final String PLANNING_SNAPSHOT = """
            WITH registry AS (
                SELECT COALESCE((SELECT epoch FROM materialization.registry_epochs WHERE tenant_id=?), 0) AS epoch
            ), published AS (
            """ + PUBLISHED_SELECT + """
                AND o.tenant_id=?
            )
            SELECT registry.epoch, published.*
            FROM registry LEFT JOIN published ON TRUE
            ORDER BY published.object_name
            """;
    private static final int MAX_CACHED_TENANTS = 256;
    private static final int MAX_CACHED_PUBLICATIONS = 4096;

    @org.pangrova.common.concurrent.GuardedBy("snapshots")
    private int cachedPublications;
    private final Map<TenantId, MaterializationPublicationSnapshot> snapshots =
            new LinkedHashMap<>(MAX_CACHED_TENANTS, 1.0f, true);
    private final ConcurrentHashMap<SnapshotRequest, CompletableFuture<MaterializationPublicationSnapshot>> pending =
            new ConcurrentHashMap<>();

    private record SnapshotRequest(TenantId tenant, long minimumRevision) { }

    private final DataSource dataSource;

    /**
     * Creates the published-object catalog over the metadata connection pool.
     *
     * @param dataSource pool that owns the materialization schema
     */
    public JdbcMaterializationCatalog(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
    }

    @Override
    public Optional<PublishedMaterialization> findPublished(MaterializationId object) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     PUBLISHED_SELECT + " AND o.tenant_id=? AND o.object_name=?")) {
            statement.setString(1, object.tenant().value());
            statement.setString(2, object.name().toString());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(map(result)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("findPublished failed", failure);
        }
    }

    @Override
    public List<PublishedMaterialization> listPublished(TenantId tenant) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     PUBLISHED_SELECT + " AND o.tenant_id=? ORDER BY o.object_name")) {
            statement.setString(1, tenant.value());
            try (ResultSet result = statement.executeQuery()) {
                List<PublishedMaterialization> published = new ArrayList<>();
                while (result.next()) {
                    published.add(map(result));
                }
                return published;
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("listPublished failed", failure);
        }
    }

    @Override
    public MaterializationPublicationSnapshot snapshot(TenantId tenant, OptionalLong admissionRevision) {
        if (admissionRevision.isEmpty()) {
            return retain(loadSnapshot(tenant));
        }
        long revision = admissionRevision.orElseThrow();
        if (revision == 0) {
            return new MaterializationPublicationSnapshot(tenant, revision, List.of());
        }
        if (revision < 0) {
            throw new IllegalArgumentException("Publication admission revision must be non-negative");
        }
        var cached = cachedSnapshot(tenant, revision);
        if (cached != null) {
            return cached;
        }
        var request = new SnapshotRequest(tenant, revision);
        var loading = new CompletableFuture<MaterializationPublicationSnapshot>();
        var existing = pending.putIfAbsent(request, loading);
        if (existing != null) {
            return awaitSnapshot(existing);
        }
        try {
            var snapshot = cachedSnapshot(tenant, revision);
            if (snapshot == null) {
                snapshot = loadSnapshot(tenant);
                if (snapshot.revision() < revision) {
                    throw MaterializationException.repositoryFailure(
                            "Publication snapshot predates admitted revision for tenant " + tenant.value(), null);
                }
                snapshot = retain(snapshot);
            }
            loading.complete(snapshot);
            return snapshot;
        } catch (RuntimeException | Error failure) {
            loading.completeExceptionally(failure);
            throw failure;
        } finally {
            pending.remove(request, loading);
        }
    }

    private MaterializationPublicationSnapshot cachedSnapshot(TenantId tenant, long minimumRevision) {
        synchronized (snapshots) {
            var cached = snapshots.get(tenant);
            return cached != null && cached.revision() >= minimumRevision ? cached : null;
        }
    }

    private MaterializationPublicationSnapshot retain(MaterializationPublicationSnapshot loaded) {
        synchronized (snapshots) {
            var current = snapshots.get(loaded.tenant());
            if (current != null && current.revision() >= loaded.revision()) {
                return current;
            }
            if (current != null) {
                cachedPublications -= current.publications().size();
                snapshots.remove(loaded.tenant());
            }
            if (loaded.publications().size() > MAX_CACHED_PUBLICATIONS) {
                return loaded;
            }
            snapshots.put(loaded.tenant(), loaded);
            cachedPublications += loaded.publications().size();
            var victims = snapshots.values().iterator();
            while (cachedPublications > MAX_CACHED_PUBLICATIONS || snapshots.size() > MAX_CACHED_TENANTS) {
                cachedPublications -= victims.next().publications().size();
                victims.remove();
            }
            return loaded;
        }
    }

    private MaterializationPublicationSnapshot loadSnapshot(TenantId tenant) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(PLANNING_SNAPSHOT)) {
            statement.setString(1, tenant.value());
            statement.setString(2, tenant.value());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new SQLException("Tenant publication snapshot omitted its revision");
                }
                long revision = result.getLong("epoch");
                List<PublishedMaterialization> publications = new ArrayList<>();
                do {
                    if (result.getString("object_name") != null) {
                        publications.add(map(result));
                    }
                } while (result.next());
                return new MaterializationPublicationSnapshot(tenant, revision, publications);
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("Publication snapshot load failed", failure);
        }
    }

    private static MaterializationPublicationSnapshot awaitSnapshot(
            CompletableFuture<MaterializationPublicationSnapshot> loading) {
        try {
            return loading.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failure.getCause() instanceof Error error) {
                throw error;
            }
            throw failure;
        }
    }

    @Override
    public long registryEpoch(TenantId tenant) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT epoch FROM materialization.registry_epochs WHERE tenant_id=?")) {
            statement.setString(1, tenant.value());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? result.getLong("epoch") : 0L;
            }
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("registryEpoch failed", failure);
        }
    }

    private PublishedMaterialization map(ResultSet result) throws SQLException {
        MaterializationId object = new MaterializationId(
                new TenantId(result.getString("tenant_id")),
                QualifiedName.parse(result.getString("object_name")));
        JsonNode schemaNode = MaterializationJson.tree(result.getString("output_schema"));
        OutputSchema schema = MaterializationJson.outputSchema(schemaNode);
        Set<SourceRef> sources = MaterializationJson.sources(
                MaterializationJson.tree(result.getString("sources")));
        SourceVector sourceVector = MaterializationJson.sourceVector(
                MaterializationJson.tree(result.getString("source_vector")));
        Instant publishedAt = getInstant(result, "published_at");
        if (publishedAt == null) {
            throw MaterializationJson.corrupt(
                    "published generation lacks a publication instant for " + object.canonicalName());
        }
        return new PublishedMaterialization(
                object,
                MaterializationKind.valueOf(result.getString("object_kind")),
                new GenerationId(result.getObject("published_generation_id", java.util.UUID.class)),
                new PhysicalTableRef(result.getString("physical_catalog"),
                        result.getString("physical_schema"), result.getString("physical_table")),
                result.getLong("definition_revision"),
                result.getString("definition_sql"),
                schema,
                sources,
                refreshKind(result.getString("refresh_kind")),
                sourceVector,
                publishedAt);
    }

    private static RefreshKind refreshKind(String value) {
        return switch (value) {
            case "FULL" -> RefreshKind.FULL;
            case "INCREMENTAL_APPEND_ONLY" -> RefreshKind.INCREMENTAL_APPEND_ONLY;
            case "INCREMENTAL_PARTITION_REPLACE" -> RefreshKind.INCREMENTAL_PARTITION_REPLACE;
            case "INCREMENTAL_AFFECTED_KEY" -> RefreshKind.INCREMENTAL_AFFECTED_KEY;
            case "INCREMENTAL_CDC" -> RefreshKind.INCREMENTAL_CDC;
            default -> throw MaterializationJson.corrupt("unknown refresh kind " + value);
        };
    }
}
