package org.pangrova.metadata.dynamictable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** JDBC authority for Dynamic Table definitions and atomic generation heads. */
public final class JdbcDynamicTablePublicationStore {

    private static final String COLUMNS = "tenant_id, logical_name, incarnation_id, definition_sql, "
            + "binding_catalog, binding_schema, published_table, revision, lifecycle_state, "
            + "created_at, updated_at";

    private final DataSource dataSource;

    /** Opens a publication authority on the migrated metadata database. */
    public JdbcDynamicTablePublicationStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /** Reserves a logical identity before any physical output is created. */
    public void create(String tenantId, String logicalName, String definitionSql,
            String bindingCatalog, String bindingSchema) {
        String sql = "INSERT INTO dynamic_table_publications (" + COLUMNS
                + ") VALUES (?, ?, ?, ?, ?, ?, NULL, 0, 'CREATING', ?, ?)";
        Instant now = Instant.now();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, logicalName);
            statement.setObject(3, UUID.randomUUID());
            statement.setString(4, definitionSql);
            statement.setString(5, bindingCatalog);
            statement.setString(6, bindingSchema);
            statement.setTimestamp(7, Timestamp.from(now));
            statement.setTimestamp(8, Timestamp.from(now));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create Dynamic Table definition " + logicalName, e);
        }
    }

    /** Removes only an unpublished, unchanged creation after a failed initial refresh. */
    public void abandonCreation(DynamicTablePublication expected) {
        String sql = "DELETE FROM dynamic_table_publications WHERE tenant_id = ? "
                + "AND logical_name = ? AND incarnation_id = ? AND revision = 0 "
                + "AND lifecycle_state = 'CREATING'";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, expected.tenantId());
            statement.setString(2, expected.logicalName());
            statement.setObject(3, UUID.fromString(expected.incarnationId()));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot abandon Dynamic Table " + expected.logicalName(), e);
        }
    }

    /** Publishes a committed candidate only if the definition/head has not changed. */
    public void publish(DynamicTablePublication expected, String committedTable) {
        Objects.requireNonNull(committedTable, "committedTable");
        String sql = "UPDATE dynamic_table_publications SET published_table = ?, "
                + "revision = revision + 1, lifecycle_state = 'ACTIVE', updated_at = ? "
                + "WHERE tenant_id = ? AND logical_name = ? AND revision = ? "
                + "AND incarnation_id = ? AND definition_sql = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, committedTable);
            statement.setTimestamp(2, Timestamp.from(Instant.now()));
            statement.setString(3, expected.tenantId());
            statement.setString(4, expected.logicalName());
            statement.setLong(5, expected.revision());
            statement.setObject(6, UUID.fromString(expected.incarnationId()));
            statement.setString(7, expected.definitionSql());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Dynamic Table publication changed during refresh: "
                        + expected.logicalName());
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot publish Dynamic Table " + expected.logicalName(), e);
        }
    }

    /** Loads one tenant-scoped logical definition and its current head. */
    public Optional<DynamicTablePublication> load(String tenantId, String logicalName) {
        String sql = "SELECT " + COLUMNS + " FROM dynamic_table_publications "
                + "WHERE tenant_id = ? AND logical_name = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            statement.setString(2, logicalName);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(read(result)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot load Dynamic Table " + logicalName, e);
        }
    }

    /** Lists only definitions belonging to the requested tenant. */
    public List<DynamicTablePublication> list(String tenantId) {
        String sql = "SELECT " + COLUMNS + " FROM dynamic_table_publications WHERE tenant_id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tenantId);
            try (ResultSet result = statement.executeQuery()) {
                List<DynamicTablePublication> publications = new ArrayList<>();
                while (result.next()) {
                    publications.add(read(result));
                }
                return List.copyOf(publications);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot list Dynamic Tables for tenant " + tenantId, e);
        }
    }

    /** Removes a specific logical incarnation; old candidate tables require separate retention cleanup. */
    public void delete(DynamicTablePublication expected) {
        String sql = "DELETE FROM dynamic_table_publications WHERE tenant_id = ? "
                + "AND logical_name = ? AND revision = ? AND incarnation_id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, expected.tenantId());
            statement.setString(2, expected.logicalName());
            statement.setLong(3, expected.revision());
            statement.setObject(4, UUID.fromString(expected.incarnationId()));
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Dynamic Table changed during drop: " + expected.logicalName());
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot drop Dynamic Table " + expected.logicalName(), e);
        }
    }

    private static DynamicTablePublication read(ResultSet result) throws SQLException {
        return new DynamicTablePublication(
                result.getString("tenant_id"), result.getString("logical_name"),
                result.getString("incarnation_id"),
                result.getString("definition_sql"), result.getString("binding_catalog"),
                result.getString("binding_schema"), result.getString("published_table"),
                result.getLong("revision"),
                DynamicTablePublication.State.valueOf(result.getString("lifecycle_state")),
                result.getTimestamp("created_at").toInstant(),
                result.getTimestamp("updated_at").toInstant());
    }
}
