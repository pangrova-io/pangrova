package org.pangrova.persistence.jdbc.file.profile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.spi.file.storage.FileStorageException;
import org.pangrova.persistence.jdbc.security.transaction.AuthorizationTransactionGate;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.persistence.api.file.profile.StorageProfileRegistry;
import org.pangrova.persistence.api.file.profile.StorageProfileRevision;
import org.pangrova.common.storage.StorageType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** PostgreSQL authority for current storage profiles and immutable historical definitions. */
public final class JdbcStorageProfileRegistry implements StorageProfileRegistry {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, String>> OPTIONS = new TypeReference<>() {};
    private static final String COLUMNS =
            "name, incarnation_id, revision, storage_type, bucket, object_prefix, endpoint, region, options";
    private static final String INSERT_CURRENT = "INSERT INTO storage_profile_definitions (" + COLUMNS
            + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (name) DO NOTHING";

    private final DataSource dataSource;

    public JdbcStorageProfileRegistry(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
    }

    @Override
    public void register(StorageProfile profile) {
        throw new UnsupportedOperationException("Storage profiles require atomic createIfAbsent");
    }

    @Override
    public boolean createIfAbsent(StorageProfile profile) {
        Objects.requireNonNull(profile, "profile must not be null");
        if (!profile.credentials().isEmpty()) {
            throw new IllegalArgumentException("Storage profile credentials must be Worker-local secret references");
        }
        StorageProfileRevision snapshot = new StorageProfileRevision(
                profile, UUID.randomUUID().toString(), 1);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(connection);
            try (PreparedStatement current = connection.prepareStatement(INSERT_CURRENT)) {
                bind(current, snapshot);
                if (current.executeUpdate() == 0) {
                    connection.commit();
                    return false;
                }
                connection.commit();
                return true;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new FileStorageException("Failed to create storage profile: " + profile.name(), failure);
        }
    }

    @Override
    public boolean unregister(String name) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(connection);
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM storage_profile_definitions WHERE name=?")) {
                statement.setString(1, name);
                boolean removed = statement.executeUpdate() == 1;
                connection.commit();
                return removed;
            } catch (SQLException | RuntimeException failure) {
                rollback(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new FileStorageException("Failed to drop storage profile: " + name, failure);
        }
    }

    @Override
    public Optional<StorageProfile> get(String name) {
        return getCurrentRevision(name).map(StorageProfileRevision::profile);
    }

    @Override
    public Optional<StorageProfileRevision> getCurrentRevision(String name) {
        return queryOne("SELECT " + COLUMNS + " FROM storage_profile_definitions WHERE name=?",
                statement -> statement.setString(1, name));
    }

    @Override
    public Optional<StorageProfileRevision> findRevision(
            String name, String incarnationId, long revision) {
        if (name == null || name.isBlank() || incarnationId == null
                || incarnationId.isBlank() || revision <= 0) {
            return Optional.empty();
        }
        UUID incarnation = UUID.fromString(incarnationId);
        return queryOne("SELECT " + COLUMNS + " FROM storage_profile_revisions "
                + "WHERE name=? AND incarnation_id=? AND revision=?", statement -> {
                    statement.setString(1, name);
                    statement.setObject(2, incarnation);
                    statement.setLong(3, revision);
                });
    }

    @Override
    public List<StorageProfile> list() {
        List<StorageProfile> result = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT " + COLUMNS + " FROM storage_profile_definitions ORDER BY name");
                ResultSet rows = statement.executeQuery()) {
            while (rows.next()) {
                result.add(read(rows).profile());
            }
            return List.copyOf(result);
        } catch (SQLException failure) {
            throw new FileStorageException("Failed to list storage profiles", failure);
        }
    }

    @Override
    public Optional<StorageProfile> resolveByUrl(String url) {
        StorageProfile match = null;
        int longest = -1;
        for (StorageProfile candidate : list()) {
            if (candidate.matchesUrl(url)) {
                int length = candidate.prefix().length();
                if (length == longest) {
                    throw new FileStorageException("Ambiguous storage profiles match source URL");
                }
                if (length > longest) {
                    match = candidate;
                    longest = length;
                }
            }
        }
        return Optional.ofNullable(match);
    }

    private Optional<StorageProfileRevision> queryOne(String sql, Binder binder) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            binder.bind(statement);
            try (ResultSet rows = statement.executeQuery()) {
                return rows.next() ? Optional.of(read(rows)) : Optional.empty();
            }
        } catch (SQLException failure) {
            throw new FileStorageException("Failed to read storage profile revision", failure);
        }
    }

    private static void bind(PreparedStatement statement, StorageProfileRevision snapshot)
            throws SQLException {
        StorageProfile profile = snapshot.profile();
        statement.setString(1, profile.name());
        statement.setObject(2, UUID.fromString(snapshot.incarnationId()));
        statement.setLong(3, snapshot.revision());
        statement.setString(4, profile.type().name());
        statement.setString(5, profile.bucket());
        statement.setString(6, profile.prefix());
        statement.setString(7, profile.endpoint());
        statement.setString(8, profile.region());
        try {
            statement.setObject(9, JSON.writeValueAsString(profile.options()), Types.OTHER);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("Storage profile options cannot be encoded", failure);
        }
    }

    private static StorageProfileRevision read(ResultSet rows) throws SQLException {
        Map<String, String> options;
        try {
            options = JSON.readValue(rows.getString("options"), OPTIONS);
        } catch (JsonProcessingException failure) {
            throw new FileStorageException("Stored storage profile options are invalid", failure);
        }
        StorageProfile profile = new StorageProfile(rows.getString("name"),
                StorageType.valueOf(rows.getString("storage_type")), rows.getString("bucket"),
                rows.getString("object_prefix"), rows.getString("endpoint"),
                rows.getString("region"), Map.of(), options);
        return new StorageProfileRevision(profile,
                rows.getObject("incarnation_id", UUID.class).toString(), rows.getLong("revision"));
    }

    private static void rollback(Connection connection, Throwable failure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            failure.addSuppressed(rollbackFailure);
        }
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }
}
