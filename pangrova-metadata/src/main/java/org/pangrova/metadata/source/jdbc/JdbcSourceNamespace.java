package org.pangrova.metadata.source.jdbc;

import org.pangrova.connector.spi.metadata.ManagedSourceCatalog;

import java.net.URI;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

/** Checks old immutable source objects against the actual current catalog storage namespace. */
final class JdbcSourceNamespace {
    private static final int MAX_NAMESPACE_LENGTH = 8192;

    private JdbcSourceNamespace() {}

    static void requireCurrent(Connection connection, ManagedSourceCatalog.Deployment deployment, String datasetUri)
        throws SQLException {
        try (var statement = connection.prepareStatement("SELECT properties->>'warehouse' FROM catalog_definitions "
            + "WHERE name=? AND incarnation_id=? AND revision=? AND state='ACTIVE' FOR SHARE")) {
            statement.setString(1, deployment.catalog());
            statement.setString(2, deployment.incarnation());
            statement.setLong(3, deployment.revision());
            try (var result = statement.executeQuery()) {
                if (!result.next()) throw new SecurityException("Current source deployment is unavailable");
                var namespace = location(result.getString(1));
                var dataset = location(datasetUri);
                if (!namespace.kind().equals(dataset.kind()) || !Objects.equals(namespace.authority(), dataset
                    .authority())
                    || dataset.path().equals(namespace.path()) || !dataset.path().startsWith(namespace.path())) {
                    throw new SecurityException("Accepted source objects are outside the current catalog namespace");
                }
            }
        }
    }

    private static Location location(String value) {
        if (value == null || value.isBlank() || value.length() > MAX_NAMESPACE_LENGTH || value.contains("%")
            || value.contains("\\") || value.contains("?") || value.contains("#")) {
            throw new SecurityException("Source catalog needs an explicit canonical storage namespace");
        }
        var uri = URI.create(value);
        if (uri.getScheme() == null) return local(Path.of(value));
        if ("file".equals(uri.getScheme()) && uri.getAuthority() == null && value.startsWith("file:///")) {
            return local(Path.of(uri));
        }
        return remote(uri);
    }

    private static Location remote(URI uri) {
        if (!"s3".equals(uri.getScheme()) || uri.getHost() == null || uri.getUserInfo() != null
            || uri.getPort() != -1 || !uri.normalize().equals(uri)) {
            throw new SecurityException("Unsupported or noncanonical source storage namespace");
        }
        var path = Path.of(uri.getPath().isEmpty() ? "/" : uri.getPath());
        if (!path.isAbsolute() || !path.normalize().equals(path)) {
            throw new SecurityException("Source namespace traverses paths");
        }
        return new Location("s3", uri.getHost(), path);
    }

    private static Location local(Path path) {
        if (!path.isAbsolute() || !path.normalize().equals(path)) {
            throw new SecurityException("Source namespace traverses paths");
        }
        return new Location("file", null, path);
    }
    private record Location(String kind, String authority, Path path) {}
}
