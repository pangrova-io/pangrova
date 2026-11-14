package org.pangrova.persistence.catalog.postgresql.catalog;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import org.pangrova.common.json.CanonicalJsonMapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.pangrova.metadata.api.cache.InvalidationEntry.InvalidationType;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.setJson;

import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.persistence.catalog.postgresql.transaction.PostgresCatalogPersistenceSupport;

/** PostgreSQL implementation of {@link CatalogDeploymentRepository}; borrows the DataSource. */
public final class PostgresCatalogDeploymentRepository extends PostgresCatalogPersistenceSupport
        implements CatalogDeploymentRepository {

    public PostgresCatalogDeploymentRepository(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
        // A later revision is an update of a live predecessor. An upsert here would
        // recreate an already dropped incarnation when there is no current row.
        final String sql = descriptor.revision() == 1
                ? "INSERT INTO catalog_definitions (name, type, provider, state, properties, comment, "
                        + "allowed_tenants, incarnation_id, revision) VALUES (?,?,?,?,?,?,?,?,?) "
                        + "ON CONFLICT (name) DO NOTHING"
                : "UPDATE catalog_definitions SET type=?, provider=?, state=?, properties=?, comment=?, "
                        + "allowed_tenants=?, revision=? WHERE name=? AND incarnation_id=? AND revision=?";
        final String historySql = "INSERT INTO catalog_descriptor_revisions "
                + "(name, type, provider, state, properties, comment, allowed_tenants, "
                + "incarnation_id, revision) VALUES (?,?,?,?,?,?,?,?,?)";
        executeMutation("save catalog descriptor", descriptor.name(), InvalidationType.CATALOG, conn -> {
            try (PreparedStatement current = conn.prepareStatement(sql);
                    PreparedStatement history = conn.prepareStatement(historySql)) {
                if (descriptor.revision() == 1) {
                    bindSaveCatalog(current, descriptor);
                } else {
                    bindCatalogUpdate(current, descriptor);
                }
                if (current.executeUpdate() != 1) {
                    throw new MetastoreException(
                            "Catalog revision fence rejected save: " + descriptor.name());
                }
                bindSaveCatalog(history, descriptor);
                history.executeUpdate();
                return true;
            }
        });
    }

    @Override
    public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
        String sql = "SELECT name, type, provider, state, properties, comment, allowed_tenants, "
                + "incarnation_id, revision "
                + "FROM catalog_definitions WHERE name=?";
        return executeQuery(sql, ps -> ps.setString(1, name),
                rs -> rs.next() ? Optional.of(mapCatalogDescriptor(rs)) : Optional.empty());
    }

    @Override
    public Optional<CatalogDescriptor> findCatalogDescriptorRevision(
            String name, String incarnationId, long revision) {
        if (name == null || name.isBlank() || incarnationId == null
                || incarnationId.isBlank() || revision <= 0) {
            return Optional.empty();
        }
        String sql = "SELECT name, type, provider, state, properties, comment, allowed_tenants, "
                + "incarnation_id, revision FROM catalog_descriptor_revisions "
                + "WHERE name=? AND incarnation_id=? AND revision=?";
        return executeQuery(sql, ps -> {
            ps.setString(1, name);
            ps.setString(2, incarnationId);
            ps.setLong(3, revision);
        }, rs -> rs.next() ? Optional.of(mapCatalogDescriptor(rs)) : Optional.empty());
    }

    @Override
    public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) {
        String sql = "DELETE FROM catalog_definitions WHERE name=? AND incarnation_id=? AND revision=?";
        return executeMutation("delete catalog descriptor", expected.catalogName(), InvalidationType.CATALOG,
                conn -> executeUpdate(conn, sql, ps -> {
                    ps.setString(1, expected.catalogName());
                    ps.setString(2, expected.incarnationId());
                    ps.setLong(3, expected.revision());
                }) == 1);
    }

    @Override
    public List<CatalogDescriptor> listAllCatalogDescriptors() {
        String sql = "SELECT name, type, provider, state, properties, comment, allowed_tenants, "
                + "incarnation_id, revision "
                + "FROM catalog_definitions";
        return executeQuery(sql, ps -> { }, this::mapCatalogDescriptors);
    }

    private void bindSaveCatalog(PreparedStatement ps, CatalogDescriptor d) throws SQLException {
        ps.setString(1, d.name());
        ps.setString(2, d.type().name());
        ps.setString(3, d.provider());
        ps.setString(4, d.state().name());
        setJson(ps, 5, toJson(d.properties()));
        ps.setString(6, d.comment());
        setJson(ps, 7, toJson(d.allowedTenants()));
        ps.setString(8, d.incarnationId());
        ps.setLong(9, d.revision());
    }

    private void bindCatalogUpdate(PreparedStatement ps, CatalogDescriptor d) throws SQLException {
        ps.setString(1, d.type().name());
        ps.setString(2, d.provider());
        ps.setString(3, d.state().name());
        setJson(ps, 4, toJson(d.properties()));
        ps.setString(5, d.comment());
        setJson(ps, 6, toJson(d.allowedTenants()));
        ps.setLong(7, d.revision());
        ps.setString(8, d.name());
        ps.setString(9, d.incarnationId());
        ps.setLong(10, d.revision() - 1);
    }

    private CatalogDescriptor mapCatalogDescriptor(ResultSet rs) throws SQLException {
        return new CatalogDescriptor(
                rs.getString("name"),
                CatalogType.valueOf(rs.getString("type")),
                rs.getString("provider"),
                CatalogState.valueOf(rs.getString("state")),
                fromJsonMap(rs.getString("properties")),
                rs.getString("comment"),
                fromJsonSet(rs.getString("allowed_tenants")),
                rs.getString("incarnation_id"),
                rs.getLong("revision"));
    }

    private List<CatalogDescriptor> mapCatalogDescriptors(ResultSet rs) throws SQLException {
        List<CatalogDescriptor> list = new ArrayList<>();
        while (rs.next()) {
            list.add(mapCatalogDescriptor(rs));
        }
        return list;
    }

    private Map<String, String> fromJsonMap(String json) {
        if (json == null) return Map.of();
        try {
            return CanonicalJsonMapper.readValue(json, new TypeReference<>() { });
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON deserialization failed", e);
        }
    }

    private Set<String> fromJsonSet(String json) {
        if (json == null) return Set.of();
        try {
            return CanonicalJsonMapper.readValue(json, new TypeReference<>() { });
        } catch (JsonProcessingException e) {
            throw new MetastoreException("JSON deserialization failed", e);
        }
    }
}
