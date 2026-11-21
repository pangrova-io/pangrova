package org.pangrova.persistence.materialization.postgresql;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
public final class JdbcMaterializationCatalog implements MaterializationCatalog {

    private static final String PUBLISHED_SELECT =
            "SELECT o.tenant_id, o.object_name, o.object_kind, o.published_generation_id, "
                    + "o.definition_revision, g.physical_catalog, g.physical_schema, "
                    + "g.physical_table, g.refresh_kind, g.source_vector, g.published_at, "
                    + "d.definition_sql, d.output_schema, d.sources, d.resolved "
                    + "FROM materialization.objects o "
                    + "JOIN materialization.generations g "
                    + "ON g.generation_id=o.published_generation_id "
                    + "JOIN materialization.definition_revisions d "
                    + "ON d.tenant_id=o.tenant_id AND d.object_name=o.object_name "
                    + "AND d.revision=g.definition_revision "
                    + "WHERE o.state='ACTIVE' AND d.resolved";

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
