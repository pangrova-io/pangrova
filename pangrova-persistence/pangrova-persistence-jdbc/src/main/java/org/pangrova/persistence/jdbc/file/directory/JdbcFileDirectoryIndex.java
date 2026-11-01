package org.pangrova.persistence.jdbc.file.directory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;

import org.pangrova.spi.file.storage.FileStorageException;
import org.pangrova.common.jdbc.JdbcSupport;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.persistence.api.file.directory.FileDirectoryIndex;
import org.pangrova.persistence.api.file.directory.FileDirectoryIndex.RefreshToken;
import org.pangrova.spi.file.directory.FileDirectoryReference;
import org.pangrova.spi.file.space.FileSpace;
import org.pangrova.common.file.FileSpaceType;
import org.pangrova.filesystem.api.object.FileStat;

/**
 * Durable directory observations. Pages remain private until a terminal page is
 * recorded and atomically published. New refreshes fence old attempts; failed or
 * partial scans never remove published files. No file bytes are stored here.
 */
public final class JdbcFileDirectoryIndex implements FileDirectoryIndex {
    private static final String COLUMNS = "file_space_id, relative_path, size_bytes, "
            + "last_modified, etag, content_type, provider_version, md5";
    private final DataSource source;

    public JdbcFileDirectoryIndex(DataSource source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    /** Resolves an already authorized Stage definition to its immutable scan identity. */
    @Override
    public FileDirectoryReference reference(FileSpace space) {
        return transaction(connection -> reference(connection, space));
    }

    private static FileDirectoryReference reference(Connection connection, FileSpace space) throws SQLException {
        if (space.type() != FileSpaceType.STAGE || !space.directoryEnabled()) {
            throw invalid("Stage directory is not enabled");
        }
        try (var query = connection.prepareStatement("SELECT file_space_id,updated_at FROM file_space_registry "
                + "WHERE space_type='STAGE' AND catalog_name=? AND schema_name=? AND space_name=? "
                + "AND directory_enabled AND created_at=? AND updated_at=? AND file_space_id=? FOR SHARE")) {
            query.setString(1, space.catalog());
            query.setString(2, space.schema());
            query.setString(3, space.name());
            JdbcSupport.setInstant(query, 4, space.createdAt());
            JdbcSupport.setInstant(query, 5, space.updatedAt());
            query.setObject(6, space.id());
            try (var row = query.executeQuery()) {
                if (!row.next()) throw invalid("Stage definition changed before directory binding");
                return new FileDirectoryReference(row.getObject(1, UUID.class),
                        JdbcSupport.getInstant(row, "updated_at"));
            }
        }
    }

    /** Starts a full or directory-scoped refresh and fences an earlier unfinished scan. */
    @Override
    public RefreshToken begin(FileSpace space, String subpath) {
        String prefix = FileDirectoryIndex.normalizeSubpath(subpath);
        return transaction(connection -> {
            UUID id;
            try (var query = connection.prepareStatement("SELECT file_space_id, updated_at, created_at, "
                    + "directory_enabled FROM file_space_registry WHERE space_type='STAGE' "
                    + "AND catalog_name=? AND schema_name=? AND space_name=? FOR UPDATE")) {
                query.setString(1, space.catalog());
                query.setString(2, space.schema());
                query.setString(3, space.name());
                try (var row = query.executeQuery()) {
                    if (!row.next() || !row.getBoolean("directory_enabled")) {
                        throw invalid("Stage directory is not enabled");
                    }
                    if (!row.getObject("file_space_id", UUID.class).equals(space.id())
                            || !JdbcSupport.getInstant(row, "updated_at").equals(space.updatedAt())
                            || !JdbcSupport.getInstant(row, "created_at").equals(space.createdAt())) {
                        throw invalid("Stage definition changed before refresh");
                    }
                    id = row.getObject("file_space_id", UUID.class);
                }
            }
            deleteRefresh(connection, id);
            UUID refresh = UUID.randomUUID();
            try (var insert = connection.prepareStatement("INSERT INTO file_directory_refresh "
                    + "(file_space_id, refresh_id, space_updated_at, subpath) VALUES (?,?,?,?)")) {
                insert.setObject(1, id);
                insert.setObject(2, refresh);
                JdbcSupport.setInstant(insert, 3, space.updatedAt());
                insert.setString(4, prefix);
                insert.executeUpdate();
            }
            return new RefreshToken(id, refresh);
        });
    }

    /** Records exactly the expected page and its checkpoint in one transaction. */
    @Override
    public void append(RefreshToken token, String expectedPageToken, FileListing page) {
        Objects.requireNonNull(page, "page");
        if (page.files().size() > 1000) {
            throw invalid("Directory refresh pages must contain at most 1000 objects");
        }
        transaction(connection -> {
            Progress progress = lockProgress(connection, token);
            if (progress.complete() || !Objects.equals(progress.nextPageToken(), expectedPageToken)) {
                throw invalid("Directory refresh page is stale or already complete");
            }
            if (page.nextPageToken() != null && (page.nextPageToken().isEmpty()
                    || page.nextPageToken().equals(expectedPageToken))) {
                throw invalid("Directory refresh continuation did not advance");
            }
            try (var insert = connection.prepareStatement("INSERT INTO file_directory_pending ("
                    + COLUMNS + ") VALUES (?,?,?,?,?,?,?,?) ON CONFLICT (file_space_id, relative_path) "
                    + "DO UPDATE SET size_bytes=excluded.size_bytes,last_modified=excluded.last_modified,"
                    + "etag=excluded.etag,content_type=excluded.content_type,"
                    + "provider_version=excluded.provider_version,md5=excluded.md5")) {
                for (FileStat file : page.files()) {
                    FileDirectoryIndex.validatePath(file.path());
                    if (!within(file.path(), progress.subpath())) {
                        throw invalid("Directory observation is outside the refresh subpath");
                    }
                    bind(insert, token.spaceId(), file);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (var update = connection.prepareStatement("UPDATE file_directory_refresh SET "
                    + "next_page_token=?, page_number=page_number+1, scan_complete=? WHERE file_space_id=?")) {
                update.setString(1, page.nextPageToken());
                update.setBoolean(2, page.nextPageToken() == null);
                update.setObject(3, token.spaceId());
                update.executeUpdate();
            }
            return null;
        });
    }

    /** Publishes a complete scan; deletion is restricted to its exact directory boundary. */
    @Override
    public long publish(RefreshToken token) {
        return transaction(connection -> {
            Progress progress = lockProgress(connection, token);
            if (!progress.complete()) {
                throw invalid("An incomplete directory scan cannot be published");
            }
            try (var delete = connection.prepareStatement("DELETE FROM file_directory_index "
                    + "WHERE file_space_id=? AND (?='' OR relative_path=? OR starts_with(relative_path,?))")) {
                delete.setObject(1, token.spaceId());
                delete.setString(2, progress.subpath());
                delete.setString(3, progress.subpath());
                delete.setString(4, progress.subpath() + "/");
                delete.executeUpdate();
            }
            long count;
            try (var insert = connection.prepareStatement("INSERT INTO file_directory_index ("
                    + COLUMNS + ") SELECT " + COLUMNS + " FROM file_directory_pending WHERE file_space_id=?")) {
                insert.setObject(1, token.spaceId());
                count = insert.executeLargeUpdate();
            }
            deleteRefresh(connection, token.spaceId());
            return count;
        });
    }

    /** Abandons only this attempt, even if a newer refresh has already replaced it. */
    @Override
    public void abort(RefreshToken token) {
        transaction(connection -> {
            try (var delete = connection.prepareStatement("DELETE FROM file_directory_refresh "
                    + "WHERE file_space_id=? AND refresh_id=?")) {
                delete.setObject(1, token.spaceId());
                delete.setObject(2, token.refreshId());
                delete.executeUpdate();
            }
            return null;
        });
    }

    /** Reads published metadata for diagnostics; this is not a relational SQL execution path. */
    @Override
    public List<FileStat> page(FileSpace space, String after, int limit) {
        if (limit < 1 || limit > 1000) {
            throw new IllegalArgumentException("Directory page size must be between 1 and 1000");
        }
        return transaction(connection -> {
            // Hold the captured definition through this diagnostic page. A same-name
            // replacement must not receive authority from the caller's old FileSpace.
            reference(connection, space);
            List<FileStat> files = new ArrayList<>();
            try (var query = connection.prepareStatement("SELECT * FROM file_directory_index "
                    + "WHERE file_space_id=? AND relative_path > ? ORDER BY relative_path LIMIT ?")) {
                query.setObject(1, space.id());
                query.setString(2, after == null ? "" : after);
                query.setInt(3, limit);
                try (var row = query.executeQuery()) {
                    while (row.next()) {
                        files.add(new FileStat(row.getString("relative_path"), row.getLong("size_bytes"),
                                JdbcSupport.getInstant(row, "last_modified"), row.getString("etag"),
                                row.getString("content_type"), row.getString("provider_version"),
                                row.getString("md5")));
                    }
                }
            }
            return List.copyOf(files);
        });
    }

    private static Progress lockProgress(Connection connection, RefreshToken token) throws SQLException {
        Instant revision;
        // Use the same lock order as DDL/begin, including when a stage is dropped.
        try (var query = connection.prepareStatement("SELECT updated_at, directory_enabled "
                + "FROM file_space_registry WHERE file_space_id=? FOR UPDATE")) {
            query.setObject(1, token.spaceId());
            try (var row = query.executeQuery()) {
                if (!row.next() || !row.getBoolean("directory_enabled")) {
                    throw invalid("Stage was dropped or its directory disabled");
                }
                revision = JdbcSupport.getInstant(row, "updated_at");
            }
        }
        try (var query = connection.prepareStatement("SELECT * FROM file_directory_refresh "
                + "WHERE file_space_id=? AND refresh_id=? FOR UPDATE")) {
            query.setObject(1, token.spaceId());
            query.setObject(2, token.refreshId());
            try (var row = query.executeQuery()) {
                if (!row.next()) {
                    throw invalid("Directory refresh was superseded or finished");
                }
                if (!revision.equals(JdbcSupport.getInstant(row, "space_updated_at"))) {
                    throw invalid("Stage definition changed during directory refresh");
                }
                return new Progress(row.getString("subpath"), row.getString("next_page_token"),
                        row.getBoolean("scan_complete"));
            }
        }
    }

    private static void bind(PreparedStatement statement, UUID id, FileStat file) throws SQLException {
        statement.setObject(1, id);
        statement.setString(2, file.path());
        statement.setLong(3, file.size());
        JdbcSupport.setInstant(statement, 4, file.lastModified());
        statement.setString(5, file.etag());
        statement.setString(6, file.contentType());
        statement.setString(7, file.providerVersion());
        statement.setString(8, file.md5());
    }

    private static void deleteRefresh(Connection connection, UUID id) throws SQLException {
        try (var delete = connection.prepareStatement("DELETE FROM file_directory_refresh WHERE file_space_id=?")) {
            delete.setObject(1, id);
            delete.executeUpdate();
        }
    }

    private <T> T transaction(JdbcSupport.SqlFunction<Connection, T> action) {
        try (var connection = source.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = action.apply(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException failure) {
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    failure.addSuppressed(rollback);
                }
                throw failure;
            }
        } catch (SQLException failure) {
            throw new FileStorageException("Directory index transaction failed", failure);
        }
    }

    private static boolean within(String path, String prefix) {
        return prefix.isEmpty() || path.equals(prefix) || path.startsWith(prefix + "/");
    }

    private static FileStorageException invalid(String message) {
        return new FileStorageException(message);
    }

    private record Progress(String subpath, String nextPageToken, boolean complete) {}
}
