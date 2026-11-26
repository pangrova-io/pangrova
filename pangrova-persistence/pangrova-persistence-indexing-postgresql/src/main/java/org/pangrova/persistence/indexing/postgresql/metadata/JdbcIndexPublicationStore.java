package org.pangrova.persistence.indexing.postgresql.metadata;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.rollbackAfterFailure;

import org.pangrova.common.util.DigestUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest;
import org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest;
import org.pangrova.indexing.api.enums.IndexPublicationMode;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;

import static org.pangrova.persistence.jdbc.support.JdbcSupport.canonicalInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.getInstant;
import static org.pangrova.persistence.jdbc.support.JdbcSupport.setInstant;

/** Transactional validation and activation of immutable index generations. */
final class JdbcIndexPublicationStore extends JdbcStoreSupport {

    private final JdbcIndexDescriptorStore descriptors;

    JdbcIndexPublicationStore(DataSource dataSource, JdbcIndexDescriptorStore descriptors) {
        super(dataSource);
        this.descriptors = java.util.Objects.requireNonNull(descriptors, "descriptors");
    }

    IndexGenerationManifest publish(IndexPublicationRequest request) {
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                AuthorizationTransactionGate.lockPublication(connection);
                try (PreparedStatement schemaLock = connection.prepareStatement(
                        "SELECT pangrova_assert_index_schema_descriptor_available(?)")) {
                    schemaLock.setString(1, request.indexId());
                    schemaLock.executeQuery().close();
                }
                DescriptorFence descriptor = lockDescriptor(connection, request.indexId());
                descriptors.requireCurrentScalarPublicationAuthority(
                        connection, request.indexId());
                List<IndexFileMapping> mappings = loadMappings(connection, request);
                Optional<IndexGenerationManifest> existing =
                        loadGeneration(connection, request.indexId(), request.generation());
                if (isIdempotentRetry(descriptor, request, mappings, existing)) {
                    connection.commit();
                    return existing.orElseThrow();
                }
                IndexPublicationMode mode = validatePublicationFence(
                        connection, descriptor, request);
                IndexGenerationManifest manifest = buildManifest(
                        request, mappings, nextPublicationEpoch(connection, request.indexId()));
                retireActiveGeneration(connection, request.indexId());
                insertManifest(connection, manifest);
                activateDescriptor(connection, descriptor, request, mode);
                if (mode == IndexPublicationMode.ONLINE_REPLACEMENT) {
                    completeBuildingGeneration(connection, request);
                }
                connection.commit();
                return manifest;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to publish index generation", failure);
        }
    }

    boolean retireNativeGenerationForSourceDrift(
            IndexDescriptor expected, IndexGenerationManifest active,
            String currentSnapshot, Instant timestamp) {
        if ((expected.indexType() != IndexType.VECTOR && expected.indexType() != IndexType.INVERTED)
                || expected.state() != IndexState.READY
                || !expected.indexId().equals(active.indexId())
                || expected.generation() != active.generation()
                || !active.sourceSnapshot().equals(expected.buildSnapshotId())
                || active.sourceSnapshot().isBlank()
                || currentSnapshot == null || currentSnapshot.isBlank()
                || active.sourceSnapshot().equals(currentSnapshot)) {
            return false;
        }
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                AuthorizationTransactionGate.lockPublication(connection);
                try (PreparedStatement schemaLock = connection.prepareStatement(
                        "SELECT pangrova_assert_index_schema_descriptor_available(?)")) {
                    schemaLock.setString(1, expected.indexId());
                    schemaLock.executeQuery().close();
                }
                boolean retired = retireDriftedManifest(connection, expected, active, timestamp);
                if (retired) {
                    connection.commit();
                } else {
                    connection.rollback();
                }
                return retired;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to retire drifted native generation", failure);
        }
    }

    private static boolean retireDriftedManifest(
            Connection connection, IndexDescriptor expected,
            IndexGenerationManifest active, Instant timestamp) throws SQLException {
        // Take the same descriptor lock as publication and build reservation before
        // examining staging state; a concurrent reservation must not be invalidated.
        String lock = "SELECT 1 FROM index_descriptors WHERE index_id=? AND tenant_id=? "
                + "AND index_type=? AND state='READY' AND generation=? AND updated_at=? "
                + "AND build_snapshot_id=? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(lock)) {
            statement.setString(1, expected.indexId());
            statement.setString(2, expected.tenantId());
            statement.setString(3, expected.indexType().name());
            statement.setInt(4, expected.generation());
            setInstant(statement, 5, expected.updatedAt());
            statement.setString(6, active.sourceSnapshot());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return false;
                }
            }
        }
        String updateDescriptor = "UPDATE index_descriptors SET state='STALE', "
                + "stale_reason='native_source_snapshot_changed', updated_at=? WHERE index_id=? "
                + "AND NOT EXISTS (SELECT 1 FROM index_generation_builds "
                + "WHERE index_id=? AND state='BUILDING')";
        try (PreparedStatement statement = connection.prepareStatement(updateDescriptor)) {
            setInstant(statement, 1, timestamp);
            statement.setString(2, expected.indexId());
            statement.setString(3, expected.indexId());
            if (statement.executeUpdate() != 1) {
                return false;
            }
        }
        String retire = "UPDATE index_generation_manifests SET state='RETIRED' "
                + "WHERE index_id=? AND generation=? AND source_snapshot=? "
                + "AND source_id=? AND artifact_type=? "
                + "AND publication_epoch=? AND manifest_sha256=? AND state='ACTIVE'";
        try (PreparedStatement statement = connection.prepareStatement(retire)) {
            statement.setString(1, active.indexId());
            statement.setInt(2, active.generation());
            statement.setString(3, active.sourceSnapshot());
            statement.setString(4, active.sourceId());
            statement.setString(5, active.artifactType());
            statement.setLong(6, active.publicationEpoch());
            statement.setString(7, active.manifestSha256());
            return statement.executeUpdate() == 1;
        }
    }

    Optional<IndexGenerationManifest> findActive(String indexId) {
        String sql = "SELECT * FROM index_generation_manifests "
                + "WHERE index_id=? AND state='ACTIVE'";
        return executeQuery(sql, statement -> statement.setString(1, indexId),
                result -> result.next()
                        ? Optional.of(mapManifest(result))
                        : Optional.empty());
    }

    private static DescriptorFence lockDescriptor(
            Connection connection, String indexId) throws SQLException {
        String sql = "SELECT state, generation, updated_at FROM index_descriptors "
                + "WHERE index_id=? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException("Index disappeared before publication: " + indexId);
                }
                return new DescriptorFence(
                        IndexState.valueOf(result.getString("state")),
                        result.getInt("generation"),
                        getInstant(result, "updated_at"));
            }
        }
    }

    private static IndexPublicationMode validatePublicationFence(
            Connection connection,
            DescriptorFence descriptor,
            IndexPublicationRequest request) throws SQLException {
        if (IndexPublicationMode.matchesInitialBuild(descriptor.state(), descriptor.generation(),
                descriptor.updatedAt(), request.generation(), request.expectedUpdatedAt())) {
            return IndexPublicationMode.INITIAL_BUILD;
        }
        if (IndexPublicationMode.isReplacementCandidate(
                descriptor.state(), descriptor.generation(), request.generation())
                && hasExpectedBuildingGeneration(connection, request, descriptor.generation())) {
            return IndexPublicationMode.ONLINE_REPLACEMENT;
        }
        throw new IllegalStateException(
                "Index publication fence does not match initial or staging generation: "
                        + request.indexId());
    }

    private static boolean hasExpectedBuildingGeneration(
            Connection connection,
            IndexPublicationRequest request,
            int baseGeneration) throws SQLException {
        String sql = "SELECT 1 FROM index_generation_builds WHERE index_id=? "
                + "AND generation=? AND base_generation=? AND source_snapshot=? "
                + "AND state='BUILDING' AND updated_at=? FOR UPDATE";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, request.indexId());
            statement.setInt(2, request.generation());
            statement.setInt(3, baseGeneration);
            statement.setString(4, request.sourceSnapshot());
            setInstant(statement, 5, request.expectedUpdatedAt());
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static List<IndexFileMapping> loadMappings(
            Connection connection, IndexPublicationRequest request) throws SQLException {
        String sql = "SELECT * FROM index_file_mappings "
                + "WHERE index_id=? AND generation=? ORDER BY data_file_path";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, request.indexId());
            statement.setInt(2, request.generation());
            try (ResultSet result = statement.executeQuery()) {
                List<IndexFileMapping> mappings = IndexMetadataMapper.mapMappingList(result);
                validateMappings(request, mappings);
                return mappings;
            }
        }
    }

    private static void validateMappings(
            IndexPublicationRequest request, List<IndexFileMapping> mappings) {
        if (mappings.size() != request.expectedObjectCount()) {
            throw new IllegalStateException(
                    "Index generation object count mismatch: expected="
                            + request.expectedObjectCount() + " actual=" + mappings.size());
        }
    }

    private static IndexGenerationManifest buildManifest(
            IndexPublicationRequest request,
            List<IndexFileMapping> mappings,
            long publicationEpoch) {
        long rowCount = mappings.stream()
                .mapToLong(IndexFileMapping::rowCount)
                .reduce(0L, Math::addExact);
        long totalBytes = mappings.stream()
                .mapToLong(IndexFileMapping::indexSizeBytes)
                .reduce(0L, Math::addExact);
        String digest = manifestDigest(request, mappings);
        return new IndexGenerationManifest(
                request.indexId(), request.sourceId(), request.generation(),
                request.sourceSnapshot(),
                request.artifactType(), request.format(), mappings.size(),
                rowCount, totalBytes, digest,
                publicationEpoch, canonicalInstant(request.publishedAt()));
    }

    private static long nextPublicationEpoch(
            Connection connection, String indexId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COALESCE(MAX(publication_epoch),0) FROM index_generation_manifests "
                        + "WHERE index_id=?")) {
            statement.setString(1, indexId);
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return Math.addExact(result.getLong(1), 1L);
            }
        }
    }

    private static String manifestDigest(
            IndexPublicationRequest request, List<IndexFileMapping> mappings) {
        MessageDigest digest = DigestUtils.sha256();
        update(digest, request.indexId());
        update(digest, request.sourceId());
        update(digest, Integer.toString(request.generation()));
        update(digest, request.sourceSnapshot());
        update(digest, request.artifactType());
        update(digest, request.format().formatId());
        update(digest, Integer.toString(request.format().majorVersion()));
        update(digest, Integer.toString(request.format().minorVersion()));
        update(digest, request.format().schemaSha256());
        mappings.stream().sorted(Comparator.comparing(IndexFileMapping::dataFilePath))
                .forEach(mapping -> updateMapping(digest, mapping));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void updateMapping(MessageDigest digest, IndexFileMapping mapping) {
        update(digest, mapping.dataFilePath());
        update(digest, mapping.indexFilePath());
        update(digest, mapping.checksum());
        update(digest, Long.toString(mapping.rowCount()));
        update(digest, Long.toString(mapping.indexSizeBytes()));
        update(digest, mapping.snapshotToken());
        update(digest, Long.toString(mapping.sequenceNum()));
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static void retireActiveGeneration(
            Connection connection, String indexId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE index_generation_manifests SET state='RETIRED' "
                        + "WHERE index_id=? AND state='ACTIVE'")) {
            statement.setString(1, indexId);
            statement.executeUpdate();
        }
    }

    private static void insertManifest(
            Connection connection, IndexGenerationManifest manifest) throws SQLException {
        String sql = "INSERT INTO index_generation_manifests "
                + "(index_id,generation,source_id,source_snapshot,artifact_type,format_id,"
                + "format_major,format_minor,schema_sha256,writer_build_id,object_count,"
                + "row_count,total_bytes,manifest_sha256,publication_epoch,state,published_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            bindManifest(statement, manifest);
            statement.executeUpdate();
        }
    }

    private static void bindManifest(
            PreparedStatement statement, IndexGenerationManifest manifest) throws SQLException {
        int parameter = 1;
        statement.setString(parameter++, manifest.indexId());
        statement.setInt(parameter++, manifest.generation());
        statement.setString(parameter++, manifest.sourceId());
        statement.setString(parameter++, manifest.sourceSnapshot());
        statement.setString(parameter++, manifest.artifactType());
        statement.setString(parameter++, manifest.format().formatId());
        statement.setInt(parameter++, manifest.format().majorVersion());
        statement.setInt(parameter++, manifest.format().minorVersion());
        statement.setString(parameter++, manifest.format().schemaSha256());
        statement.setString(parameter++, manifest.format().writerBuildId());
        statement.setLong(parameter++, manifest.objectCount());
        statement.setLong(parameter++, manifest.rowCount());
        statement.setLong(parameter++, manifest.totalBytes());
        statement.setString(parameter++, manifest.manifestSha256());
        statement.setLong(parameter++, manifest.publicationEpoch());
        statement.setString(parameter++, "ACTIVE");
        setInstant(statement, parameter, manifest.publishedAt());
    }

    private static void activateDescriptor(
            Connection connection,
            DescriptorFence descriptor,
            IndexPublicationRequest request,
            IndexPublicationMode mode) throws SQLException {
        String sql = "UPDATE index_descriptors SET state=?, generation=?, "
                + "build_snapshot_id=?, stale_reason=NULL, last_indexed_snapshot_id=?, updated_at=? "
                + "WHERE index_id=? AND state=? AND generation=? AND updated_at=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, IndexState.READY.name());
            statement.setInt(2, request.generation());
            statement.setString(3, request.sourceSnapshot());
            statement.setString(4, request.sourceSnapshot());
            setInstant(statement, 5, request.publishedAt());
            statement.setString(6, request.indexId());
            statement.setString(7, descriptor.state().name());
            statement.setInt(8, descriptor.generation());
            setInstant(statement, 9, descriptor.updatedAt());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException(
                        "Index activation fence was lost for " + mode.name().toLowerCase(java.util.Locale.ROOT));
            }
        }
    }

    private static void completeBuildingGeneration(
            Connection connection, IndexPublicationRequest request) throws SQLException {
        String sql = "UPDATE index_generation_builds SET state='PUBLISHED', updated_at=?, "
                + "failure_reason=NULL WHERE index_id=? AND generation=? "
                + "AND state='BUILDING' AND updated_at=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            setInstant(statement, 1, request.publishedAt());
            statement.setString(2, request.indexId());
            statement.setInt(3, request.generation());
            setInstant(statement, 4, request.expectedUpdatedAt());
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Index staging-generation fence was lost");
            }
        }
    }

    private static Optional<IndexGenerationManifest> loadGeneration(
            Connection connection, String indexId, int generation) throws SQLException {
        String sql = "SELECT * FROM index_generation_manifests "
                + "WHERE index_id=? AND generation=? AND state='ACTIVE'";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexId);
            statement.setInt(2, generation);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(mapManifest(result)) : Optional.empty();
            }
        }
    }

    private static boolean isIdempotentRetry(
            DescriptorFence descriptor,
            IndexPublicationRequest request,
            List<IndexFileMapping> mappings,
            Optional<IndexGenerationManifest> existing) {
        if (existing.isEmpty()
                || descriptor.state() != IndexState.READY
                || descriptor.generation() != request.generation()) {
            return false;
        }
        IndexGenerationManifest candidate = buildManifest(
                request, mappings, existing.get().publicationEpoch());
        if (!samePublishedIdentity(existing.get(), candidate)) {
            throw new IllegalStateException(
                    "Published generation differs from idempotent retry: " + request.indexId());
        }
        return true;
    }

    private static boolean samePublishedIdentity(
            IndexGenerationManifest left, IndexGenerationManifest right) {
        return sameManifestIdentity(left, right)
                && sameManifestTotals(left, right);
    }

    private static boolean sameManifestIdentity(
            IndexGenerationManifest left, IndexGenerationManifest right) {
        return left.indexId().equals(right.indexId())
                && left.sourceId().equals(right.sourceId())
                && left.generation() == right.generation()
                && left.sourceSnapshot().equals(right.sourceSnapshot())
                && left.artifactType().equals(right.artifactType())
                && left.format().equals(right.format());
    }

    private static boolean sameManifestTotals(
            IndexGenerationManifest left, IndexGenerationManifest right) {
        return left.objectCount() == right.objectCount()
                && left.rowCount() == right.rowCount()
                && left.totalBytes() == right.totalBytes()
                && left.manifestSha256().equals(right.manifestSha256());
    }

    private static IndexGenerationManifest mapManifest(ResultSet result) throws SQLException {
        IndexGenerationFormat format = new IndexGenerationFormat(
                result.getString("format_id"), result.getInt("format_major"),
                result.getInt("format_minor"), result.getString("schema_sha256"),
                result.getString("writer_build_id"));
        return new IndexGenerationManifest(
                result.getString("index_id"), result.getString("source_id"),
                result.getInt("generation"),
                result.getString("source_snapshot"), result.getString("artifact_type"),
                format, result.getLong("object_count"), result.getLong("row_count"),
                result.getLong("total_bytes"), result.getString("manifest_sha256"),
                result.getLong("publication_epoch"),
                getInstant(result, "published_at"));
    }

    private record DescriptorFence(IndexState state, int generation, java.time.Instant updatedAt) {}
}
