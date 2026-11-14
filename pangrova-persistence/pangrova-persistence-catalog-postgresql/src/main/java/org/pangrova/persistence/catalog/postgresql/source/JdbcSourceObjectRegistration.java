package org.pangrova.persistence.catalog.postgresql.source;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.connector.api.metadata.SourceCatalogDeployment;

import org.pangrova.connector.api.metadata.PreparedSourcePublication;
import org.pangrova.connector.api.metadata.SourceObjectPage;
import org.pangrova.connector.api.metadata.SourcePublicationReservation;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

/** Bounded exact page receipts and streamed object-membership sealing on the locked candidate transaction. */
final class JdbcSourceObjectRegistration {
    private JdbcSourceObjectRegistration() {}

    static void register(Connection connection, SourceCatalogDeployment deployment,
        SourcePublicationReservation reservation, PreparedSourcePublication header,
        SourceObjectPage page) throws SQLException {
        initializeHeader(connection, reservation, header);
        try (var statement = connection.prepareStatement("SELECT next_object_page,registered_count,state "
            + "FROM managed_source_publications WHERE fingerprint=?")) {
            statement.setString(1, reservation.fingerprint());
            try (var result = statement.executeQuery()) {
                if (!result.next()) throw new IllegalStateException("Source candidate is absent");
                int next = result.getInt(1);
                if (page.index() < next) {
                    requirePageReceipt(connection, reservation.fingerprint(), page);
                    return;
                }
                if (page.index() != next || !result.getString(3).equals("PREPARING")
                    || result.getLong(2) + page.objects().size() > header.objectsCount()) {
                    throw new IllegalStateException("Source object page has a hole, exceeds its total or "
                        + "follows sealing");
                }
            }
        }
        if (page.index() > 0) requirePreviousCursor(connection, reservation.fingerprint(), page);
        for (var object : page.objects()) {
            requireUnaccepted(connection, deployment, reservation, object.relativePath());
            try (var statement = connection
                .prepareStatement("INSERT INTO "
                + "managed_source_objects(fingerprint,relative_path,size_bytes,purpose) VALUES(?,?,?,?)")) {
                statement.setString(1, reservation.fingerprint());
                statement.setString(2, object.relativePath());
                statement.setLong(3, object.sizeBytes());
                statement.setString(4, object.purpose().name());
                statement.executeUpdate();
            }
        }
        try (var statement = connection
            .prepareStatement("INSERT INTO managed_source_object_pages VALUES(?,?,?,?,?,?,?)")) {
            statement.setString(1, reservation.fingerprint());
            statement.setInt(2, page.index());
            statement.setString(3, page.afterPath());
            statement.setString(4, page.lastPath());
            statement.setInt(5, page.objects().size());
            statement.setBytes(6, page.sha256());
            statement.setBoolean(7, page.last());
            statement.executeUpdate();
        }
        try (var statement = connection
            .prepareStatement("UPDATE managed_source_publications SET next_object_page=next_object_page+1,"
            + "registered_count=registered_count+? WHERE fingerprint=? AND state='PREPARING'")) {
            statement.setInt(1, page.objects().size());
            statement.setString(2, reservation.fingerprint());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Source object registration lost its candidate");
            }
        }
        if (page.last()) seal(connection, reservation, header);
    }

    private static void initializeHeader(Connection connection, SourcePublicationReservation reservation,
        PreparedSourcePublication header) throws SQLException {
        try (var statement = connection.prepareStatement("UPDATE managed_source_publications SET manifest_sha256=?,"
            + "manifest_size=?,naming_scheme=?,row_count=?,object_count=?,objects_sha256=? "
            + "WHERE fingerprint=? AND state='PREPARING' AND manifest_sha256 IS NULL")) {
            statement.setBytes(1, header.candidate().manifestSha256());
            statement.setLong(2, header.candidate().manifestSize());
            statement.setString(3, header.candidate().namingScheme());
            statement.setLong(4, header.rows());
            statement.setLong(5, header.objectsCount());
            statement.setBytes(6, header.objectsSha256());
            statement.setString(7, reservation.fingerprint());
            statement.executeUpdate();
        }
        requireHeader(connection, reservation, header, false);
    }

    static void requireHeader(Connection connection, SourcePublicationReservation reservation,
        PreparedSourcePublication header, boolean sealed) throws SQLException {
        try (var statement = connection
            .prepareStatement("SELECT * FROM managed_source_publications WHERE fingerprint=?")) {
            statement.setString(1, reservation.fingerprint());
            try (var result = statement.executeQuery()) {
                if (!result.next() || (sealed && !result.getString("state").equals("PREPARED"))
                    || !Arrays.equals(result.getBytes("manifest_sha256"), header.candidate().manifestSha256())
                    || result.getLong("manifest_size") != header.candidate().manifestSize()
                    || !header.candidate().namingScheme().equals(result.getString("naming_scheme"))
                    || result.getLong("row_count") != header.rows() || result.getLong("object_count") != header
                    .objectsCount()
                    || !Arrays.equals(result.getBytes("objects_sha256"), header.objectsSha256())
                    || !header.inheritedMembership().equals(java.util.Objects.requireNonNullElse(
                    result.getString("inherited_membership"), ""))) {
                    throw new IllegalStateException("Source preparation header differs from the durable candidate");
                }
            }
        }
    }

    private static void requirePreviousCursor(Connection connection, String fingerprint,
        SourceObjectPage page) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT last_path,is_last FROM managed_source_object_pages "
            + "WHERE fingerprint=? AND page_index=?")) {
            statement.setString(1, fingerprint);
            statement.setInt(2, page.index() - 1);
            try (var result = statement.executeQuery()) {
                if (!result.next() || !result.getString(1).equals(page.afterPath()) || result.getBoolean(2)) {
                    throw new IllegalStateException("Source object page cursor differs from the exact "
                        + "previous receipt");
                }
            }
        }
    }

    private static void requirePageReceipt(Connection connection, String fingerprint,
        SourceObjectPage page) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT after_path,last_path,object_count,page_sha256,is_last "
            + "FROM managed_source_object_pages WHERE fingerprint=? AND page_index=?")) {
            statement.setString(1, fingerprint);
            statement.setInt(2, page.index());
            try (var result = statement.executeQuery()) {
                if (!result.next() || !result.getString(1).equals(page.afterPath()) || !result.getString(2)
                    .equals(page.lastPath())
                    || result.getInt(3) != page.objects().size() || !Arrays.equals(result.getBytes(4), page
                    .sha256())
                    || result.getBoolean(5) != page.last()) {
                    throw new IllegalStateException("Source object page replay conflicts with its immutable receipt");
                }
            }
        }
    }

    private static void requireUnaccepted(Connection connection, SourceCatalogDeployment deployment,
        SourcePublicationReservation reservation, String path) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT 1 FROM managed_source_objects o "
            + "JOIN managed_source_publications p USING(fingerprint) WHERE p.catalog_name=? "
            + "AND p.catalog_incarnation=? AND p.tenant_id=? AND p.table_id=? AND o.relative_path=? "
            + "AND (p.state='ACCEPTED' OR o.cleanup_id IS NOT NULL) LIMIT 1")) {
            statement.setString(1, deployment.catalog());
            statement.setString(2, deployment.incarnation());
            statement.setString(3, reservation.base().key().tenant());
            statement.setString(4, reservation.base().tableId());
            statement.setString(5, path);
            try (var result = statement.executeQuery()) {
                if (result.next()) throw new IllegalStateException("New source object is owned by accepted history");
            }
        }
    }

    private static void seal(Connection connection, SourcePublicationReservation reservation,
        PreparedSourcePublication header)
        throws SQLException {
        java.security.MessageDigest digest;
        digest = DigestUtils.sha256();
        long count = 0;
        boolean manifest = false;
        try (var statement = connection
            .prepareStatement("SELECT relative_path,size_bytes,purpose FROM managed_source_objects "
            + "WHERE fingerprint=? ORDER BY relative_path COLLATE \"C\"")) {
            statement.setString(1, reservation.fingerprint());
            statement.setFetchSize(SourceObjectPage.MAX_PAGE_OBJECTS);
            try (var result = statement.executeQuery()) {
                while (result.next()) {
                    var object = new SourceObjectPage.ObjectReference(result.getString(1), result.getLong(2),
                        SourceObjectPage.Purpose.valueOf(result.getString(3)));
                    digest.update(SourceObjectPage.proof(object));
                    count = Math.addExact(count, 1);
                    manifest |= object.purpose() == SourceObjectPage.Purpose.SOURCE && object.relativePath()
                        .equals(reservation.manifestRelativePath())
                        && object.sizeBytes() == header.candidate().manifestSize();
                }
            }
        }
        if (!manifest || count != header.objectsCount() || !Arrays.equals(digest.digest(), header.objectsSha256())) {
            throw new IllegalStateException("Complete source object proof omits the manifest or differs from "
                + "SDK count/digest");
        }
        try (var statement = connection.prepareStatement("UPDATE managed_source_publications SET state='PREPARED' "
            + "WHERE fingerprint=? AND state='PREPARING' AND registered_count=object_count")) {
            statement.setString(1, reservation.fingerprint());
            if (statement.executeUpdate() != 1) throw new IllegalStateException("Source object proof cannot be sealed");
        }
    }
}
