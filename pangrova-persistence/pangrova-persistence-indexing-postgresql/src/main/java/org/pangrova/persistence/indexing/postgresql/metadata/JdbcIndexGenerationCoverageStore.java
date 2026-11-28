package org.pangrova.persistence.indexing.postgresql.metadata;

import static org.pangrova.persistence.jdbc.operation.JdbcOperations.rollbackAfterFailure;

import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.persistence.catalog.api.exception.MetastoreException;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationCoverage;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Reads and validates Native source coverage from one atomically published artifact job. */
final class JdbcIndexGenerationCoverageStore extends JdbcIndexMetadataStoreBase implements IndexGenerationCoverage {

    private static final String VECTOR_INDEX = "VECTOR_INDEX";
    private static final String TEXT_INDEX = "TEXT_INDEX";
    private static final String SOURCE_OBJECT = "SOURCE_OBJECT";
    private static final String WORK_UNIT = "WORK_UNIT";
    private static final String GENERATION = "GENERATION";
    private static final String ALLOW_ZERO_ROWS = "ALLOW_ZERO_ROWS";

    JdbcIndexGenerationCoverageStore(DataSource dataSource) {
        super(dataSource);
    }

    @Override
    public Set<String> getNativeIndexSourcePaths(
            IndexDescriptor descriptor, IndexGenerationManifest active) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(active, "active");
        if (!descriptor.indexType().requiresNativeArtifact()) {
            throw new IllegalArgumentException("Native coverage requires a Native index descriptor");
        }
        if (!descriptor.indexId().equals(active.indexId()) || active.sourceSnapshot().isBlank()) {
            throw new IllegalStateException("Active Native index generation identity is incomplete");
        }
        String artifactType = descriptor.indexType() == IndexType.VECTOR ? VECTOR_INDEX : TEXT_INDEX;
        if (!artifactType.equals(active.artifactType())) {
            throw new IllegalStateException("Active Native index generation type differs from its descriptor");
        }
        try (Connection connection = dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try {
                JobRow job = lockPublishedJob(connection, descriptor, active);
                FormatContract contract = requireFormatContract(job);
                Set<String> sources = loadAndValidateCoverage(connection, job, active, contract);
                connection.commit();
                return sources;
            } catch (SQLException | RuntimeException failure) {
                rollbackAfterFailure(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new MetastoreException("Failed to read Native index generation coverage", failure);
        }
    }

    private static JobRow lockPublishedJob(
            Connection connection, IndexDescriptor descriptor, IndexGenerationManifest active)
            throws SQLException {
        String sql = "SELECT * FROM artifact_build_jobs WHERE tenant_id=? AND artifact_id=? "
                + "AND staging_generation=? AND source_snapshot=? AND state='PUBLISHED' FOR SHARE";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, descriptor.tenantId());
            statement.setString(2, active.indexId());
            statement.setString(3, Integer.toString(active.generation()));
            statement.setString(4, active.sourceSnapshot());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    throw new IllegalStateException(
                            "Active Native index generation has no durable publication");
                }
                JobRow job = mapJob(result);
                validateJobIdentity(job, active);
                return job;
            }
        }
    }

    private static JobRow mapJob(ResultSet result) throws SQLException {
        return new JobRow(
                result.getString("tenant_id"), result.getString("job_id"),
                result.getString("artifact_id"), result.getString("artifact_type"),
                result.getString("staging_generation"), result.getString("source_id"),
                result.getString("source_snapshot"), result.getString("execution_kind"),
                result.getString("dependency_set_sha256"), result.getString("empty_partition_policy"),
                result.getString("format_id"), result.getInt("format_major"),
                result.getInt("format_minor"), result.getString("schema_sha256"),
                result.getString("writer_build_id"), result.getString("encryption_key_ref"),
                result.getInt("expected_pages"), result.getInt("completed_pages"),
                result.getString("manifest_sha256"),
                requiredLong(result, "published_object_count"),
                requiredLong(result, "published_row_count"),
                requiredLong(result, "published_total_bytes"),
                result.getTimestamp("published_at") != null);
    }

    private static long requiredLong(ResultSet result, String column) throws SQLException {
        long value = result.getLong(column);
        if (result.wasNull()) {
            throw new IllegalStateException("Published Native artifact counter is missing: " + column);
        }
        return value;
    }

    private static void validateJobIdentity(JobRow job, IndexGenerationManifest active) {
        if (!job.artifactId().equals(active.indexId())
                || !job.generation().equals(Integer.toString(active.generation()))
                || !job.sourceId().equals(active.sourceId())
                || !job.sourceSnapshot().equals(active.sourceSnapshot())
                || !job.artifactType().equals(active.artifactType())
                || !job.manifestSha256().equalsIgnoreCase(active.manifestSha256())
                || !job.formatId().equals(active.format().formatId())
                || job.formatMajor() != active.format().majorVersion()
                || job.formatMinor() != active.format().minorVersion()
                || !job.schemaSha256().equalsIgnoreCase(active.format().schemaSha256())
                || !job.writerBuildId().equals(active.format().writerBuildId())
                || job.publishedObjectCount() != active.objectCount()
                || job.publishedRowCount() != active.rowCount()
                || job.publishedTotalBytes() != active.totalBytes()
                || !job.published()) {
            throw new IllegalStateException("Active Native index manifest differs from its durable publication");
        }
        if (job.expectedPages() < 0 || job.expectedPages() != job.completedPages()) {
            throw new IllegalStateException("Published Native artifact has not crossed its all-pages barrier");
        }
    }

    private static FormatContract requireFormatContract(JobRow job) {
        String formatId = job.formatId();
        if (isVectorFormat(formatId)) {
            return vectorContract(job, formatId);
        }
        if (formatId.equals("pangrova.bm25.segment")) {
            return textContract(job);
        }
        throw new IllegalStateException("Active Native index generation has an unknown artifact format");
    }

    private static boolean isVectorFormat(String formatId) {
        return formatId.equals("pangrova.diskann.segment")
                || formatId.equals("pangrova.hnsw.segment")
                || formatId.equals("pangrova.ivfpq.segment");
    }

    private static FormatContract vectorContract(JobRow job, String formatId) {
        if (!VECTOR_INDEX.equals(job.artifactType())) {
            throw new IllegalStateException("Native vector format differs from its artifact type");
        }
        String graphRole = switch (formatId) {
            case "pangrova.diskann.segment" -> "DISKANN_GRAPH";
            case "pangrova.hnsw.segment" -> "HNSW_GRAPH";
            default -> "IVF_PQ_INDEX";
        };
        return new FormatContract(Map.of(
                graphRole, new Role(0, SOURCE_OBJECT),
                "RAW_VECTORS", new Role(1, SOURCE_OBJECT),
                "ROW_LOCATORS", new Role(2, SOURCE_OBJECT)), Map.of(), Map.of(), false);
    }

    private static FormatContract textContract(JobRow job) {
        if (!TEXT_INDEX.equals(job.artifactType())) {
            throw new IllegalStateException("Native text format differs from its artifact type");
        }
        return new FormatContract(Map.of(
                "BM25_POSTINGS", new Role(0, SOURCE_OBJECT),
                "ROW_LOCATORS", new Role(1, SOURCE_OBJECT),
                "BM25_STATS_SHARD", new Role(2, WORK_UNIT)),
                Map.of("BM25_STATS_SHARD", new Role(0, WORK_UNIT)),
                Map.of("BM25_CORPUS_STATS", new Role(0, GENERATION)), true);
    }

    private static Set<String> loadAndValidateCoverage(
            Connection connection, JobRow job, IndexGenerationManifest active, FormatContract contract)
            throws SQLException {
        List<PageRow> pages = loadPages(connection, job);
        Map<String, String> sourcePages = loadSourcePages(connection, job, pages);
        List<OutputRow> outputs = loadOutputs(connection, job, pages);
        if (job.expectedPages() == 0) {
            if (!pages.isEmpty() || !sourcePages.isEmpty() || !outputs.isEmpty()
                    || !ALLOW_ZERO_ROWS.equals(job.emptyPartitionPolicy())) {
                throw new IllegalStateException("Empty Native index publication is incomplete or forbidden");
            }
            validateAggregateAndDigest(job, active, List.of(), 0);
            return Set.of();
        }
        validatePagesAndOutputs(job, pages, sourcePages, outputs, contract);
        List<OutputRow> published = outputs.stream()
                .filter(output -> !WORK_UNIT.equals(output.scope()))
                .toList();
        long rows = validatePublishedBundles(job, sourcePages, published, contract);
        validateAggregateAndDigest(job, active, published, rows);
        return Set.copyOf(sourcePages.values());
    }

    private static List<PageRow> loadPages(Connection connection, JobRow job) throws SQLException {
        List<PageRow> pages = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT page_id,page_kind,state FROM artifact_build_pages "
                        + "WHERE tenant_id=? AND job_id=? ORDER BY page_ordinal")) {
            statement.setString(1, job.tenantId());
            statement.setString(2, job.jobId());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    pages.add(new PageRow(result.getString(1), result.getString(2), result.getString(3)));
                }
            }
        }
        if (pages.size() != job.expectedPages()
                || pages.stream().anyMatch(page -> !"SUCCEEDED".equals(page.state()))) {
            throw new IllegalStateException("Published Native artifact page coverage is incomplete");
        }
        return List.copyOf(pages);
    }

    private static Map<String, String> loadSourcePages(
            Connection connection, JobRow job, List<PageRow> pages) throws SQLException {
        Map<String, String> pageKinds = new HashMap<>();
        pages.forEach(page -> pageKinds.put(page.pageId(), page.kind()));
        Map<String, String> sources = new LinkedHashMap<>();
        Set<String> sourceIds = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT s.page_id,s.source_object_id FROM artifact_build_page_sources s "
                        + "WHERE s.tenant_id=? AND s.job_id=? ORDER BY s.page_id,s.source_ordinal")) {
            statement.setString(1, job.tenantId());
            statement.setString(2, job.jobId());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    String pageId = result.getString(1);
                    String sourceId = result.getString(2);
                    if (!"SOURCE_MAP".equals(pageKinds.get(pageId))
                            || sourceId == null || sourceId.isBlank()
                            || sources.put(pageId, sourceId) != null || !sourceIds.add(sourceId)) {
                        throw new IllegalStateException("Published Native source allocation is invalid");
                    }
                }
            }
        }
        Set<String> sourceMapPages = pages.stream()
                .filter(page -> "SOURCE_MAP".equals(page.kind()))
                .map(PageRow::pageId).collect(java.util.stream.Collectors.toSet());
        if (!sources.keySet().equals(sourceMapPages)) {
            throw new IllegalStateException("Published Native source pages differ from their plan");
        }
        return Map.copyOf(sources);
    }

    private static List<OutputRow> loadOutputs(
            Connection connection, JobRow job, List<PageRow> pages) throws SQLException {
        Set<String> pageIds = pages.stream().map(PageRow::pageId).collect(java.util.stream.Collectors.toSet());
        List<OutputRow> outputs = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT o.page_id,o.object_id,o.opaque_uri,o.object_role,o.source_object_id,"
                        + "o.object_ordinal,o.object_scope,o.size_bytes,o.row_count,o.checksum_sha256,"
                        + "o.encryption_key_ref FROM artifact_build_outputs o "
                        + "WHERE o.tenant_id=? AND o.job_id=? ORDER BY o.page_id,o.object_ordinal,o.object_id")) {
            statement.setString(1, job.tenantId());
            statement.setString(2, job.jobId());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    OutputRow output = new OutputRow(
                            result.getString(1), result.getString(2), result.getString(3),
                            result.getString(4), result.getString(5), result.getInt(6),
                            result.getString(7), result.getLong(8), result.getLong(9),
                            result.getString(10), result.getString("encryption_key_ref"));
                    if (!pageIds.contains(output.pageId())) {
                        throw new IllegalStateException("Published Native output has no work page");
                    }
                    outputs.add(output);
                }
            }
        }
        return List.copyOf(outputs);
    }

    private static void validatePagesAndOutputs(
            JobRow job, List<PageRow> pages, Map<String, String> sourcePages,
            List<OutputRow> outputs, FormatContract contract) {
        Map<String, List<OutputRow>> byPage = outputs.stream().collect(
                java.util.stream.Collectors.groupingBy(OutputRow::pageId, LinkedHashMap::new,
                        java.util.stream.Collectors.toList()));
        if (!byPage.keySet().equals(pages.stream().map(PageRow::pageId).collect(java.util.stream.Collectors.toSet()))) {
            throw new IllegalStateException("Published Native work pages have missing outputs");
        }
        long finalizers = 0;
        for (PageRow page : pages) {
            Map<String, Role> expected = rolesFor(contract, page.kind());
            Map<String, OutputRow> actual = new HashMap<>();
            for (OutputRow output : byPage.get(page.pageId())) {
                Role role = expected.get(output.objectRole());
                validateOutput(job, page.pageId(), sourcePages.get(page.pageId()), output, role);
                if (actual.put(output.objectRole(), output) != null) {
                    throw new IllegalStateException("Published Native work page repeats an object role");
                }
            }
            if (!actual.keySet().equals(expected.keySet())) {
                throw new IllegalStateException("Published Native work-page roles are incomplete");
            }
            if ("GENERATION_FINALIZE".equals(page.kind())) {
                finalizers++;
            }
        }
        if (finalizers != (contract.requiresFinalizer() ? 1 : 0)) {
            throw new IllegalStateException("Published Native generation finalizer count is invalid");
        }
    }

    private static Map<String, Role> rolesFor(FormatContract contract, String pageKind) {
        return switch (pageKind) {
            case "SOURCE_MAP" -> contract.sourceMapRoles();
            case "DEPENDENCY_REDUCE" -> contract.reduceRoles();
            case "GENERATION_FINALIZE" -> contract.finalizeRoles();
            default -> throw new IllegalStateException("Published Native page kind is invalid");
        };
    }

    private static void validateOutput(
            JobRow job, String pageId, String sourceId, OutputRow output, Role role) {
        if (!matchesRole(job, pageId, sourceId, output, role)) {
            throw new IllegalStateException("Published Native artifact output identity is invalid");
        }
        validateOutputContents(job, output);
    }

    private static boolean matchesRole(
            JobRow job, String pageId, String sourceId, OutputRow output, Role role) {
        if (role == null || role.ordinal() != output.ordinal() || !role.scope().equals(output.scope())) {
            return false;
        }
        String expectedScopeId = switch (output.scope()) {
            case SOURCE_OBJECT -> sourceId;
            case WORK_UNIT -> pageId;
            case GENERATION -> job.generation();
            default -> throw new IllegalStateException("Published Native output scope is invalid");
        };
        return expectedScopeId.equals(output.sourceObjectId());
    }

    private static void validateOutputContents(JobRow job, OutputRow output) {
        if (!job.encryptionKeyRef().equals(output.encryptionKeyRef())
                || output.sizeBytes() <= 0 || output.rowCount() < 0
                || output.objectId() == null || output.objectId().isBlank()
                || output.opaqueUri() == null || output.opaqueUri().isBlank()
                || output.checksumSha256() == null
                || !output.checksumSha256().matches("[0-9a-fA-F]{64}")
                || (output.rowCount() == 0 && !ALLOW_ZERO_ROWS.equals(job.emptyPartitionPolicy()))) {
            throw new IllegalStateException("Published Native artifact output contents are invalid");
        }
    }

    private static long validatePublishedBundles(
            JobRow job, Map<String, String> sourcePages, List<OutputRow> outputs,
            FormatContract contract) {
        Map<String, Map<String, OutputRow>> sources = new HashMap<>();
        Map<String, OutputRow> generation = new HashMap<>();
        for (OutputRow output : outputs) {
            if (SOURCE_OBJECT.equals(output.scope())) {
                sources.computeIfAbsent(output.sourceObjectId(), ignored -> new HashMap<>())
                        .put(output.objectRole(), output);
            } else if (GENERATION.equals(output.scope())) {
                if (generation.put(output.objectRole(), output) != null) {
                    throw new IllegalStateException("Published Native generation repeats an object role");
                }
            } else {
                throw new IllegalStateException("Published Native output has an unexpected scope");
            }
        }
        Map<String, Role> sourceRoles = contract.sourceMapRoles().entrySet().stream()
                .filter(entry -> SOURCE_OBJECT.equals(entry.getValue().scope()))
                .collect(java.util.stream.Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!sources.keySet().equals(Set.copyOf(sourcePages.values()))
                || !generation.keySet().equals(contract.finalizeRoles().keySet())) {
            throw new IllegalStateException("Published Native source or generation roles are incomplete");
        }
        long rows = 0;
        for (Map<String, OutputRow> bundle : sources.values()) {
            if (!bundle.keySet().equals(sourceRoles.keySet())) {
                throw new IllegalStateException("Published Native source roles are incomplete");
            }
            long sourceRows = bundle.values().iterator().next().rowCount();
            for (Map.Entry<String, OutputRow> entry : bundle.entrySet()) {
                if (entry.getValue().ordinal() != sourceRoles.get(entry.getKey()).ordinal()
                        || entry.getValue().rowCount() != sourceRows) {
                    throw new IllegalStateException("Published Native source role counters differ");
                }
            }
            rows = Math.addExact(rows, sourceRows);
        }
        for (OutputRow output : generation.values()) {
            if (output.rowCount() != rows) {
                throw new IllegalStateException("Published Native generation row count differs from source coverage");
            }
        }
        return rows;
    }

    private static void validateAggregateAndDigest(
            JobRow job, IndexGenerationManifest active, List<OutputRow> outputs, long rows) {
        long objectCount = outputs.size();
        long totalBytes = outputs.stream().mapToLong(OutputRow::sizeBytes).reduce(0L, Math::addExact);
        if (job.publishedObjectCount() != objectCount || active.objectCount() != objectCount
                || job.publishedRowCount() != rows || active.rowCount() != rows
                || job.publishedTotalBytes() != totalBytes || active.totalBytes() != totalBytes) {
            throw new IllegalStateException("Published Native artifact counters differ from its manifest");
        }
        String digest = manifestDigest(job, outputs);
        if (!digest.equalsIgnoreCase(job.manifestSha256()) || !digest.equalsIgnoreCase(active.manifestSha256())) {
            throw new IllegalStateException("Published Native artifact manifest digest is corrupt");
        }
    }

    private static String manifestDigest(JobRow job, List<OutputRow> outputs) {
        MessageDigest digest = Sha256Digests.sha256();
        update(digest, job.tenantId());
        update(digest, job.artifactId());
        update(digest, job.artifactType());
        update(digest, job.generation());
        update(digest, job.sourceId());
        update(digest, job.sourceSnapshot());
        update(digest, job.executionKind());
        update(digest, job.dependencySetSha256());
        update(digest, job.emptyPartitionPolicy());
        update(digest, job.formatId());
        update(digest, Integer.toString(job.formatMajor()));
        update(digest, Integer.toString(job.formatMinor()));
        update(digest, job.schemaSha256().toLowerCase(Locale.ROOT));
        update(digest, job.writerBuildId());
        outputs.stream().sorted(Comparator.comparing(OutputRow::sourceObjectId)
                        .thenComparingInt(OutputRow::ordinal).thenComparing(OutputRow::objectId))
                .forEach(output -> updateOutput(digest, output));
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void updateOutput(MessageDigest digest, OutputRow output) {
        update(digest, output.sourceObjectId());
        update(digest, output.objectRole());
        update(digest, output.objectId());
        update(digest, output.opaqueUri());
        update(digest, output.checksumSha256().toLowerCase(Locale.ROOT));
        update(digest, Long.toString(output.sizeBytes()));
        update(digest, Long.toString(output.rowCount()));
        update(digest, Integer.toString(output.ordinal()));
        update(digest, output.scope());
        update(digest, output.encryptionKeyRef());
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private record JobRow(
            String tenantId, String jobId, String artifactId, String artifactType,
            String generation, String sourceId, String sourceSnapshot, String executionKind,
            String dependencySetSha256, String emptyPartitionPolicy, String formatId,
            int formatMajor, int formatMinor, String schemaSha256, String writerBuildId,
            String encryptionKeyRef, int expectedPages, int completedPages, String manifestSha256,
            long publishedObjectCount, long publishedRowCount, long publishedTotalBytes,
            boolean published) {}

    private record PageRow(String pageId, String kind, String state) {}

    private record OutputRow(
            String pageId, String objectId, String opaqueUri, String objectRole,
            String sourceObjectId, int ordinal, String scope, long sizeBytes, long rowCount,
            String checksumSha256, String encryptionKeyRef) {}

    private record Role(int ordinal, String scope) {}

    private record FormatContract(
            Map<String, Role> sourceMapRoles, Map<String, Role> reduceRoles,
            Map<String, Role> finalizeRoles, boolean requiresFinalizer) {
        private FormatContract {
            sourceMapRoles = Map.copyOf(sourceMapRoles);
            reduceRoles = Map.copyOf(reduceRoles);
            finalizeRoles = Map.copyOf(finalizeRoles);
        }
    }
}
