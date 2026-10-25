package org.pangrova.metadata.file;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.sql.DataSource;

import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.store.JdbcSupport;
import org.pangrova.security.store.AuthorizationTransactionGate;
import org.pangrova.spi.storage.file.FileSpace;
import org.pangrova.spi.storage.file.FileSpaceDefinition;
import org.pangrova.spi.storage.file.FileSpaceMutation;
import org.pangrova.spi.storage.file.FileSpaceRegistry;
import org.pangrova.spi.storage.file.FileSpaceType;

/** PostgreSQL authority for Volume and Stage namespace definitions. */
public final class JdbcFileSpaceRegistry implements FileSpaceRegistry {

    private static final String COLUMNS = "file_space_id, space_type, catalog_name, schema_name, "
            + "space_name, is_external, storage_profile, location, managed_root, directory_enabled, comment, "
            + "owner_name, created_at, updated_at";

    private final DataSource dataSource;

    public JdbcFileSpaceRegistry(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public FileSpace create(FileSpaceDefinition definition, boolean ifNotExists) {
        Objects.requireNonNull(definition, "definition must not be null");
        String location = normalizeLocation(definition.location());
        validate(definition, location);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(connection);
            try {
                Optional<FileSpace> existing = find(connection, definition.type(),
                        definition.catalog(), definition.schema(), definition.name());
                if (existing.isPresent()) {
                    if (!ifNotExists) {
                        throw new MetastoreException(definition.type() + " already exists: "
                                + definition.catalog() + "." + definition.schema() + "."
                                + definition.name());
                    }
                    connection.commit();
                    return existing.get();
                }
                assertNoLocationConflict(connection, location, null);
                Instant now = JdbcSupport.canonicalInstant(Instant.now());
                UUID id = UUID.randomUUID();
                String managedRoot = definition.external() ? "" : "managed/"
                        + definition.type().name().toLowerCase(Locale.ROOT) + "s/" + id;
                FileSpace created = new FileSpace(id, definition.type(), definition.catalog(),
                        definition.schema(), definition.name(), definition.external(),
                        definition.storageProfile(), location, managedRoot, definition.directoryEnabled(),
                        definition.comment(), definition.owner(), now, now);
                insert(connection, id, created);
                connection.commit();
                return created;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to create " + definition.type() + " "
                    + definition.catalog() + "." + definition.schema() + "." + definition.name(),
                    failure);
        }
    }

    @Override
    public FileSpace mutate(FileSpaceType type, String catalog, String schema, String name,
            FileSpaceMutation mutation) {
        Objects.requireNonNull(mutation, "mutation must not be null");
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(connection);
            try {
                LockedFileSpace locked = lock(connection, type, catalog, schema, name);
                FileSpace current = locked.space();
                FileSpace updated = apply(current, mutation);
                if (mutation instanceof FileSpaceMutation.SetLocation) {
                    assertNoLocationConflict(connection, updated.location(), locked.id());
                }
                update(connection, current.type(), current.catalog(), current.schema(),
                        current.name(), updated);
                if (mutation instanceof FileSpaceMutation.SetLocation
                        || mutation instanceof FileSpaceMutation.SetStorageProfile
                        || mutation instanceof FileSpaceMutation.SetDirectoryEnabled) {
                    for (String table : List.of("file_directory_refresh", "file_directory_index")) {
                        try (var clear = connection.prepareStatement(
                                "DELETE FROM " + table + " WHERE file_space_id=?")) {
                            clear.setObject(1, locked.id());
                            clear.executeUpdate();
                        }
                    }
                }
                connection.commit();
                return updated;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to alter " + type + " " + catalog + "."
                    + schema + "." + name, failure);
        }
    }

    @Override
    public FileSpace rename(FileSpaceType type, String catalog, String schema, String name,
            String newName) {
        if (newName == null || newName.isBlank()) {
            throw new IllegalArgumentException("newName must not be blank");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(connection);
            try {
                LockedFileSpace locked = lock(connection, type, catalog, schema, name);
                FileSpace current = locked.space();
                if (find(connection, type, catalog, schema, newName).isPresent()) {
                    throw new MetastoreException(type + " already exists: " + catalog + "."
                            + schema + "." + newName);
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE file_space_registry SET space_name=?, updated_at=? "
                                + "WHERE file_space_id=?")) {
                    Instant now = JdbcSupport.canonicalInstant(Instant.now());
                    statement.setString(1, newName);
                    JdbcSupport.setInstant(statement, 2, now);
                    statement.setObject(3, locked.id());
                    statement.executeUpdate();
                    connection.commit();
                    return new FileSpace(current.id(), current.type(), current.catalog(), current.schema(),
                            newName, current.external(), current.storageProfile(),
                            current.location(), current.managedRoot(), current.directoryEnabled(), current.comment(),
                            current.owner(), current.createdAt(), now);
                }
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to rename " + type + " " + catalog + "."
                    + schema + "." + name, failure);
        }
    }

    @Override
    public Optional<FileSpace> find(FileSpaceType type, String catalog, String schema,
            String name) {
        try (Connection connection = dataSource.getConnection()) {
            return find(connection, type, catalog, schema, name);
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to read " + type + " " + catalog + "."
                    + schema + "." + name, failure);
        }
    }

    @Override
    public Optional<FileSpace> find(UUID id) {
        Objects.requireNonNull(id, "id must not be null");
        try (Connection connection = dataSource.getConnection();
                var query = connection.prepareStatement(
                        "SELECT " + COLUMNS + " FROM file_space_registry WHERE file_space_id=?")) {
            query.setObject(1, id);
            try (var rows = query.executeQuery()) {
                return rows.next() ? Optional.of(read(rows)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to read file space " + id, failure);
        }
    }

    @Override
    public List<FileSpace> list(FileSpaceType type, String catalog, String schema) {
        Objects.requireNonNull(type, "type must not be null");
        StringBuilder sql = new StringBuilder("SELECT " + COLUMNS
                + " FROM file_space_registry WHERE space_type=?");
        List<String> arguments = new ArrayList<>();
        arguments.add(type.name());
        if (catalog != null) {
            sql.append(" AND catalog_name=?");
            arguments.add(catalog);
        }
        if (schema != null) {
            sql.append(" AND schema_name=?");
            arguments.add(schema);
        }
        sql.append(" ORDER BY catalog_name, schema_name, space_name");
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < arguments.size(); i++) {
                statement.setString(i + 1, arguments.get(i));
            }
            try (ResultSet rows = statement.executeQuery()) {
                List<FileSpace> result = new ArrayList<>();
                while (rows.next()) {
                    result.add(read(rows));
                }
                return List.copyOf(result);
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to list " + type + " definitions", failure);
        }
    }

    @Override
    public boolean drop(FileSpaceType type, String catalog, String schema, String name,
            boolean ifExists) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(connection);
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM file_space_registry "
                            + "WHERE space_type=? AND catalog_name=? AND schema_name=? "
                            + "AND space_name=?")) {
                statement.setString(1, type.name());
                statement.setString(2, catalog);
                statement.setString(3, schema);
                statement.setString(4, name);
                boolean removed = statement.executeUpdate() == 1;
                if (!removed && !ifExists) {
                    throw new MetastoreException(type + " does not exist: " + catalog + "."
                            + schema + "." + name);
                }
                connection.commit();
                return removed;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to drop " + type + " " + catalog + "."
                    + schema + "." + name, failure);
        }
    }

    private static void validate(FileSpaceDefinition definition, String location) {
        if (definition.directoryEnabled() && definition.type() != FileSpaceType.STAGE) {
            throw new MetastoreException("Only stages support a directory projection");
        }
        if (definition.external()) {
            if (location.isEmpty()) {
                throw new MetastoreException("An external " + definition.type()
                        + " requires LOCATION/URL");
            }
            if (definition.storageProfile().isEmpty()) {
                throw new MetastoreException("An external " + definition.type()
                        + " requires a storage profile");
            }
        } else if (!location.isEmpty()) {
            throw new MetastoreException("A managed/internal " + definition.type()
                    + " cannot declare a location");
        }
    }

    static String normalizeLocation(String raw) {
        String value = raw == null ? "" : raw.trim();
        if (value.isEmpty()) {
            return "";
        }
        if (value.indexOf('\\') >= 0 || value.chars().anyMatch(Character::isWhitespace)) {
            throw new MetastoreException("Location contains invalid characters: " + raw);
        }
        if (value.contains("/../") || value.endsWith("/..") || value.endsWith("/.")) {
            throw new MetastoreException("Location must not contain path traversal: " + raw);
        }
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        if (value.indexOf("://") <= 0) {
            throw new MetastoreException("Location must be an absolute URI: " + raw);
        }
        return value;
    }

    private static FileSpace apply(FileSpace current, FileSpaceMutation mutation) {
        if (mutation instanceof FileSpaceMutation.SetDirectoryEnabled directory) {
            if (current.type() != FileSpaceType.STAGE) {
                throw new MetastoreException("Only stages support a directory projection");
            }
            return new FileSpace(current.id(), current.type(), current.catalog(), current.schema(),
                    current.name(), current.external(), current.storageProfile(),
                    current.location(), current.managedRoot(), directory.enabled(), current.comment(),
                    current.owner(), current.createdAt(), JdbcSupport.canonicalInstant(Instant.now()));
        }
        if (mutation instanceof FileSpaceMutation.SetLocation location) {
            if (!current.external()) {
                throw new MetastoreException("A managed/internal " + current.type()
                        + " cannot declare a location");
            }
            return new FileSpace(current.id(), current.type(), current.catalog(), current.schema(),
                    current.name(), current.external(), current.storageProfile(),
                    normalizeLocation(location.location()), current.managedRoot(), current.directoryEnabled(),
                    current.comment(), current.owner(), current.createdAt(),
                    JdbcSupport.canonicalInstant(Instant.now()));
        }
        String storageProfile = current.storageProfile();
        String comment = current.comment();
        String owner = current.owner();
        if (mutation instanceof FileSpaceMutation.SetStorageProfile profile) {
            storageProfile = profile.storageProfile();
        } else if (mutation instanceof FileSpaceMutation.SetComment newComment) {
            comment = newComment.comment();
        } else if (mutation instanceof FileSpaceMutation.SetOwner newOwner) {
            owner = newOwner.owner();
        }
        return new FileSpace(current.id(), current.type(), current.catalog(), current.schema(), current.name(),
                current.external(), storageProfile, current.location(), current.managedRoot(),
                current.directoryEnabled(), comment, owner, current.createdAt(),
                JdbcSupport.canonicalInstant(Instant.now()));
    }

    private static void insert(Connection connection, UUID id, FileSpace space)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO file_space_registry (" + COLUMNS
                        + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            statement.setObject(1, id);
            bind(statement, 2, space);
            statement.executeUpdate();
        }
    }

    private static void update(Connection connection, FileSpaceType type, String catalog,
            String schema, String name, FileSpace space) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE file_space_registry SET is_external=?, storage_profile=?, location=?, "
                        + "directory_enabled=?, comment=?, owner_name=?, updated_at=? "
                        + "WHERE space_type=? AND catalog_name=? AND schema_name=? "
                        + "AND space_name=?")) {
            int index = 1;
            statement.setBoolean(index++, space.external());
            statement.setString(index++, space.storageProfile());
            statement.setString(index++, space.location());
            statement.setBoolean(index++, space.directoryEnabled());
            statement.setString(index++, space.comment());
            statement.setString(index++, space.owner());
            JdbcSupport.setInstant(statement, index++, space.updatedAt());
            statement.setString(index++, type.name());
            statement.setString(index++, catalog);
            statement.setString(index++, schema);
            statement.setString(index, name);
            statement.executeUpdate();
        }
    }

    private static void bind(PreparedStatement statement, int start, FileSpace space)
            throws SQLException {
        int index = start;
        statement.setString(index++, space.type().name());
        statement.setString(index++, space.catalog());
        statement.setString(index++, space.schema());
        statement.setString(index++, space.name());
        statement.setBoolean(index++, space.external());
        statement.setString(index++, space.storageProfile());
        statement.setString(index++, space.location());
        statement.setString(index++, space.managedRoot());
        statement.setBoolean(index++, space.directoryEnabled());
        statement.setString(index++, space.comment());
        statement.setString(index++, space.owner());
        JdbcSupport.setInstant(statement, index++, space.createdAt());
        JdbcSupport.setInstant(statement, index, space.updatedAt());
    }

    private static Optional<FileSpace> find(Connection connection, FileSpaceType type,
            String catalog, String schema, String name) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT " + COLUMNS + " FROM file_space_registry WHERE space_type=? "
                        + "AND catalog_name=? AND schema_name=? AND space_name=?")) {
            statement.setString(1, type.name());
            statement.setString(2, catalog);
            statement.setString(3, schema);
            statement.setString(4, name);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(read(rows)) : Optional.empty();
            }
        }
    }

    private static LockedFileSpace lock(Connection connection, FileSpaceType type,
            String catalog, String schema, String name) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT " + COLUMNS + " FROM file_space_registry WHERE space_type=? "
                        + "AND catalog_name=? AND schema_name=? AND space_name=? FOR UPDATE")) {
            statement.setString(1, type.name());
            statement.setString(2, catalog);
            statement.setString(3, schema);
            statement.setString(4, name);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new MetastoreException(type + " does not exist: " + catalog + "."
                            + schema + "." + name);
                }
                return new LockedFileSpace(rows.getObject("file_space_id", UUID.class),
                        read(rows));
            }
        }
    }

    private static void assertNoLocationConflict(Connection connection, String location,
            UUID excludedId) throws SQLException {
        if (location.isEmpty()) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT file_space_id, location FROM file_space_registry "
                        + "WHERE length(location) > 0");
                ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                UUID id = rows.getObject(1, UUID.class);
                if (id.equals(excludedId)) {
                    continue;
                }
                String other = rows.getString(2);
                if (overlaps(location, other)) {
                    throw new MetastoreException("External location overlaps an existing "
                            + "definition: " + other);
                }
            }
        }
    }

    private static boolean overlaps(String left, String right) {
        return left.equals(right) || left.startsWith(right + "/") || right.startsWith(left + "/");
    }

    private static FileSpace read(ResultSet rows) throws SQLException {
        return new FileSpace(rows.getObject("file_space_id", UUID.class),
                FileSpaceType.valueOf(rows.getString("space_type")),
                rows.getString("catalog_name"), rows.getString("schema_name"),
                rows.getString("space_name"), rows.getBoolean("is_external"),
                rows.getString("storage_profile"), rows.getString("location"),
                rows.getString("managed_root"),
                rows.getBoolean("directory_enabled"), rows.getString("comment"),
                rows.getString("owner_name"),
                JdbcSupport.getInstant(rows, "created_at"),
                JdbcSupport.getInstant(rows, "updated_at"));
    }

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    private record LockedFileSpace(UUID id, FileSpace space) {}
}
