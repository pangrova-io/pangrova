package org.pangrova.coordinator.adapter.indexing.ddl;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.databuild.SnapshotDataBuildSplitPlanner;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.databuild.api.build.DataBuildPage;
import org.pangrova.databuild.api.DataBuildPageKind;
import org.pangrova.databuild.api.build.DataBuildSplit;
import org.pangrova.databuild.api.build.DataBuildStore;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.planner.databuild.DataBuildPlan;
import org.pangrova.planner.databuild.Bm25DataBuildPlanBuilder;
import org.pangrova.planner.databuild.VectorDataBuildPlanBuilder;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.data.type.PangrovaType;
import org.pangrova.connector.spi.data.type.VectorType;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.descriptor.IndexType;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Maintenance compiler for snapshot-pinned Native index work; persists canonical control metadata, never row bytes. */
public final class DurableIndexBuildService {

    private static final int MAX_REDUCTION_FAN_IN = 64;
    // Leave headroom for the Worker system pool and concurrent service leases.
    // The grant is a task limit, not the Worker's total process memory budget.
    private static final long DEFAULT_MEMORY_BYTES = 1L * 1024 * 1024 * 1024;
    private static final long DEFAULT_SPILL_BYTES = 2L * 1024 * 1024 * 1024;
    private static final int TASK_TIMEOUT_MINUTES = 30;
    private static final long DEFAULT_TASK_TIMEOUT_MILLIS = Duration.ofMinutes(TASK_TIMEOUT_MINUTES).toMillis();
    private static final int MAX_SEARCH_LIST = 100_000;
    private static final double DEFAULT_DISKANN_ALPHA = 1.2;
    private static final int DEFAULT_HNSW_EF_CONSTRUCTION = 200;
    private static final int MAX_COARSE_CLUSTERS = 4_096;
    private static final int DEFAULT_TRAINING_ITERATIONS = 20;
    private static final int DEFAULT_TRAINING_SAMPLES = 100_000;
    private static final int MAX_TRAINING_SAMPLES = 1_000_000;
    private static final int MAX_VECTOR_DIMENSION = 65_535;

    private final DataBuildStore store;
    private final CatalogRegistry catalogRegistry;
    private final SnapshotDataBuildSplitPlanner splitPlanner;
    private final String baseArtifactUri;

    /** Creates the sole durable Native index compiler over catalog and metadata authority. */
    public DurableIndexBuildService(
            DataBuildStore store, CatalogRegistry catalogRegistry,
            String baseArtifactUri) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.catalogRegistry = Objects.requireNonNull(
                catalogRegistry, "catalogRegistry must not be null");
        this.splitPlanner = new SnapshotDataBuildSplitPlanner(catalogRegistry);
        if (baseArtifactUri == null || baseArtifactUri.isBlank()) {
            throw new IllegalArgumentException("baseArtifactUri must not be blank");
        }
        this.baseArtifactUri = stripTrailingSlash(baseArtifactUri);
    }

    /** Returns whether this descriptor belongs to the Native artifact build plane. */
    public static boolean supports(IndexDescriptor descriptor) {
        String algorithm = normalizedAlgorithm(descriptor);
        return (descriptor.indexType() == IndexType.VECTOR
                && (algorithm.equals("DISK_ANN") || algorithm.equals("HNSW")
                || algorithm.equals("IVF_PQ")))
                || (descriptor.indexType() == IndexType.INVERTED && algorithm.equals("BM25"));
    }

    /** Persists a replayable Native index build and returns its stable job identity. */
    public String submit(
            IndexDescriptor descriptor, QualifiedTableName table, CoordinatorSessionContext context) {
        if (!supports(descriptor)) {
            throw new IllegalArgumentException("descriptor is not a supported Native index");
        }
        String tenantId = requireTenantId(descriptor.tenantId());
        if (!tenantId.equals(context.tenantId())
                || !table.catalog().equals(descriptor.catalogName())
                || !table.schema().equals(descriptor.schemaName())
                || !table.table().equals(descriptor.tableName())) {
            throw new org.pangrova.security.api.AccessDeniedException(
                    "Native index build scope does not match its durable descriptor");
        }
        ConnectorSession session = SessionContextConverter.toConnectorSession(
                context, table.catalog(), table.schema());
        return submitScoped(descriptor, table, tenantId, session);
    }

    /** Reauthorizes the original durable subject before a commit-driven replacement generation. */
    public String submitReplacement(IndexDescriptor previous, IndexDescriptor replacement) {
        var resolved = DeferredIndexAuthorityResolver.resolve(store, previous, replacement);
        return submitScoped(replacement, resolved.table(), resolved.tenantId(), resolved.session());
    }

    /** Reauthorizes the durable Native submitter for connector metadata maintenance. */
    public ConnectorSession authorizedSession(IndexDescriptor descriptor) {
        return DeferredIndexAuthorityResolver.resolveExisting(store, descriptor).session();
    }

    private String submitScoped(IndexDescriptor descriptor, QualifiedTableName table,
                                String tenantId, ConnectorSession session) {
        NativeBuildSpec build = prepareBuild(descriptor, table, session);
        DataBuildPlan plan = build.plan();
        List<DataBuildSplit> splits = splitPlanner.enumerate(
                table, descriptor.buildSnapshotId(),
                session);
        String jobId = jobId(tenantId, descriptor);
        String generation = Integer.toString(descriptor.generation());
        String stagingUri = stagingUri(tenantId, descriptor);
        String idempotencyKey = jobId + ":snapshot:" + descriptor.buildSnapshotId();
        Map<String, String> properties = descriptor.properties();
        int maxAttempts = boundedInt(properties, "max_attempts", 5, 1, 100);
        List<DataBuildPage> pages = buildPages(
                jobId, splits, build.artifactType(), maxAttempts);
        Instant now = Instant.now();
        var grant = new DataBuildDispatchSpec.ResourceGrant(
                        boundedLong(properties, "max_memory_bytes", DEFAULT_MEMORY_BYTES,
                                64L * 1024 * 1024, 64L * 1024 * 1024 * 1024),
                        boundedInt(properties, "max_threads", 1, 1, 16),
                        boundedLong(properties, "max_spill_bytes", DEFAULT_SPILL_BYTES,
                                64L * 1024 * 1024, 256L * 1024 * 1024 * 1024),
                        boundedLong(properties, "task_timeout_millis", DEFAULT_TASK_TIMEOUT_MILLIS,
                                1_000, Duration.ofDays(1).toMillis()));
        var submission = DurableIndexBuildSubmission.create(
                new DurableIndexBuildSubmission.Definition(
                        tenantId, session.getUser(), jobId, descriptor, table, plan,
                        build.artifactType(), build.properties(), pages, generation,
                        stagingUri, idempotencyKey, grant, now));
        store.createJob(submission.job(), submission.dispatch(), pages);
        return jobId;
    }

    static List<DataBuildPage> buildPages(
            String jobId, List<DataBuildSplit> splits,
            String artifactType, int maxAttempts) {
        List<DataBuildPage> pages = new ArrayList<>();
        List<String> level = new ArrayList<>(splits.size());
        for (int index = 0; index < splits.size(); index++) {
            String pageId = jobId + ":map:" + index;
            pages.add(new DataBuildPage(
                    jobId, pageId, 0, index, pages.size(),
                    DataBuildPageKind.SOURCE_MAP,
                    List.of(splits.get(index)), List.of(), maxAttempts));
            level.add(pageId);
        }
        if (!artifactType.equals(
                org.pangrova.databuild.api.DataBuildOutputTypeIds.TEXT_INDEX)
                || level.isEmpty()) {
            return List.copyOf(pages);
        }
        int stage = 1;
        while (level.size() > MAX_REDUCTION_FAN_IN) {
            List<String> next = new ArrayList<>();
            for (int offset = 0; offset < level.size(); offset += MAX_REDUCTION_FAN_IN) {
                int partition = next.size();
                String pageId = jobId + ":reduce:" + stage + ":" + partition;
                pages.add(new DataBuildPage(
                        jobId, pageId, stage, partition, pages.size(),
                        DataBuildPageKind.DEPENDENCY_REDUCE, List.of(),
                        level.subList(offset, Math.min(
                                level.size(), offset + MAX_REDUCTION_FAN_IN)),
                        maxAttempts));
                next.add(pageId);
            }
            level = next;
            stage++;
        }
        pages.add(new DataBuildPage(
                jobId, jobId + ":finalize", stage, 0, pages.size(),
                DataBuildPageKind.GENERATION_FINALIZE,
                List.of(), level, maxAttempts));
        return List.copyOf(pages);
    }

    private NativeBuildSpec prepareBuild(
            IndexDescriptor descriptor, QualifiedTableName table, ConnectorSession session) {
        String column = requireSingleColumn(descriptor);
        List<String> tableName = List.of(table.catalog(), table.schema(), table.table());
        if (descriptor.indexType() == IndexType.INVERTED) {
            requireTextColumn(table, column, session);
            String analyzer = descriptor.properties().getOrDefault(
                    "analyzer", "standard-v1").trim().toLowerCase(Locale.ROOT);
            if (!analyzer.equals("standard-v1")) {
                throw validation("Native BM25 requires analyzer=standard-v1");
            }
            return new NativeBuildSpec(
                    Bm25DataBuildPlanBuilder.build(tableName, column),
                    org.pangrova.databuild.api.DataBuildOutputTypeIds.TEXT_INDEX,
                    List.of(stringProperty("analyzer", analyzer)));
        }
        String algorithm = normalizedAlgorithm(descriptor);
        CapabilityRef buildCapability =
                BuiltInCapabilityCatalog.vectorArtifactBuildPlanRef(algorithm);
        String metric = requireMetric(descriptor.properties(), algorithm);
        VectorShape vector = resolveVectorShape(table, column, session);
        if (vector.elementType() != VectorType.ElementType.FLOAT) {
            throw validation("Native ANN build requires VECTOR(..., FLOAT)");
        }
        validateDeclaredDimension(descriptor.properties(), vector.dimension());
        return new NativeBuildSpec(
                VectorDataBuildPlanBuilder.build(
                        tableName, column, buildCapability,
                        BuiltInCapabilityCatalog.vectorArtifactFormat(algorithm)),
                org.pangrova.databuild.api.DataBuildOutputTypeIds.VECTOR_INDEX,
                vectorProperties(
                        buildCapability, descriptor.properties(), vector.dimension(), metric));
    }

    public int cancel(String artifactId, String tenantId, Instant now) {
        return store.cancelArtifactBuilds(requireTenantId(tenantId), artifactId, now);
    }

    private VectorShape resolveVectorShape(
            QualifiedTableName table, String columnName, ConnectorSession session) {
        ConnectorTableMetadata.Column column = resolveColumn(table, columnName, session);
        if (!(column.parameterizedType() instanceof VectorType vector)) {
            throw validation("Native ANN requires a fixed-dimension VECTOR column");
        }
        return new VectorShape(vector.dimension(), vector.elementType());
    }

    private void requireTextColumn(QualifiedTableName table, String columnName, ConnectorSession session) {
        if (resolveColumn(table, columnName, session).type() != PangrovaType.VARCHAR) {
            throw validation("Native BM25 requires a VARCHAR column");
        }
    }

    private ConnectorTableMetadata.Column resolveColumn(
            QualifiedTableName table, String columnName, ConnectorSession session) {
        CatalogHandle catalog = catalogRegistry.getCatalog(table.catalog());
        if (catalog == null) {
            throw validation("Catalog disappeared while resolving index column: "
                    + table.catalog());
        }
        ConnectorMetadata metadata = catalog.getMetadata(session);
        ConnectorTableHandle handle = metadata.getTableHandle(
                session, new SchemaTableName(table.schema(), table.table()));
        if (handle == null) {
            throw validation("Table disappeared while resolving index column: " + table);
        }
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, handle);
        return tableMetadata.typedColumns().stream()
                .filter(candidate -> candidate.name().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> validation("Index column disappeared: " + columnName));
    }

    private static List<NamedValue> diskAnnProperties(
            Map<String, String> properties, int dimension) {
        int maxDegree = boundedInt(properties, "max_degree", 64, 1, 512);
        int searchListSize = properties.containsKey("search_list_size")
                ? boundedInt(properties, "search_list_size", 100, maxDegree, MAX_SEARCH_LIST)
                : boundedInt(properties, "build_list_size", 100, maxDegree, MAX_SEARCH_LIST);
        double alpha = boundedDouble(properties, "alpha", DEFAULT_DISKANN_ALPHA, 1.0, 2.0);
        int pqSubvectors = properties.containsKey("pq_subvectors")
                ? boundedInt(properties, "pq_subvectors", 1, 1, dimension)
                : greatestDivisorAtMost(dimension, 32);
        if (dimension % pqSubvectors != 0) {
            throw validation("pq_subvectors must divide vector dimension exactly");
        }
        return List.of(
                intProperty("dimension", dimension),
                intProperty("max_degree", maxDegree),
                intProperty("search_list_size", searchListSize),
                doubleProperty("alpha", alpha),
                intProperty("pq_subvectors", pqSubvectors),
                intProperty("pq_bits_per_code",
                        boundedInt(properties, "pq_bits_per_code", 8, 1, 8)),
                intProperty("beam_width",
                        boundedInt(properties, "beam_width", 4, 1, 1_024)));
    }

    private static List<NamedValue> hnswProperties(
            Map<String, String> properties, int dimension) {
        int connections = boundedInt(properties, "max_connections", 16, 2, 512);
        return List.of(
                intProperty("dimension", dimension),
                intProperty("max_connections", connections),
                intProperty("ef_construction", boundedInt(
                        properties, "ef_construction", DEFAULT_HNSW_EF_CONSTRUCTION, connections, MAX_SEARCH_LIST)));
    }

    private static List<NamedValue> ivfPqProperties(
            Map<String, String> properties, int dimension) {
        int subvectors = properties.containsKey("pq_subvectors")
                ? boundedInt(properties, "pq_subvectors", 1, 1, dimension)
                : greatestDivisorAtMost(dimension, 32);
        if (dimension % subvectors != 0) {
            throw validation("pq_subvectors must divide vector dimension exactly");
        }
        return List.of(
                intProperty("dimension", dimension),
                intProperty("coarse_clusters", boundedInt(
                        properties, "coarse_clusters", 64, 1, MAX_COARSE_CLUSTERS)),
                intProperty("pq_subvectors", subvectors),
                intProperty("pq_bits_per_code", boundedInt(
                        properties, "pq_bits_per_code", 8, 1, 8)),
                intProperty("training_iterations", boundedInt(
                        properties, "training_iterations", DEFAULT_TRAINING_ITERATIONS, 1, 100)),
                intProperty("training_samples", boundedInt(
                        properties, "training_samples", DEFAULT_TRAINING_SAMPLES, 256, MAX_TRAINING_SAMPLES)));
    }

    static List<NamedValue> vectorProperties(
            CapabilityRef buildCapability,
            Map<String, String> properties, int dimension, String metric) {
        String capabilityId = buildCapability.getCapabilityId();
        List<NamedValue> result;
        if (BuiltInCapabilityCatalog.HNSW_ARTIFACT_BUILD_ID.equals(capabilityId)) {
            result = new ArrayList<>(hnswProperties(properties, dimension));
        } else if (BuiltInCapabilityCatalog.IVF_PQ_ARTIFACT_BUILD_ID.equals(capabilityId)) {
            result = new ArrayList<>(ivfPqProperties(properties, dimension));
        } else if (BuiltInCapabilityCatalog.DISKANN_ARTIFACT_BUILD_ID.equals(capabilityId)) {
            result = new ArrayList<>(diskAnnProperties(properties, dimension));
        } else {
            throw validation("Unknown Native artifact build capability");
        }
        result.add(stringProperty("metric", metric));
        return List.copyOf(result);
    }

    private static NamedValue intProperty(String name, long value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("BIGINT"))
                        .setInt64Value(value)).build();
    }

    private static NamedValue doubleProperty(String name, double value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("DOUBLE"))
                        .setDoubleValue(value)).build();
    }

    private static NamedValue stringProperty(String name, String value) {
        return NamedValue.newBuilder().setName(name).setValue(
                TypedValue.newBuilder()
                        .setType(LogicalType.newBuilder().setCanonicalSql("VARCHAR"))
                        .setStringValue(value)).build();
    }

    private static int greatestDivisorAtMost(int dimension, int ceiling) {
        for (int candidate = Math.min(dimension, ceiling); candidate >= 1; candidate--) {
            if (dimension % candidate == 0) {
                return candidate;
            }
        }
        return 1;
    }

    static String requireMetric(Map<String, String> properties, String algorithm) {
        String metric = properties.getOrDefault("metric", "L2")
                .trim().toUpperCase(Locale.ROOT);
        if (metric.equals("EUCLIDEAN")) {
            metric = "L2";
        }
        if (!metric.equals("L2") && !metric.equals("COSINE")
                && !metric.equals("INNER_PRODUCT")) {
            throw validation("Unsupported Native ANN metric: " + metric);
        }
        if (algorithm.equals("DISK_ANN") && !metric.equals("L2")) {
            throw validation("Native DiskANN requires metric=L2");
        }
        return metric;
    }

    private static void validateDeclaredDimension(Map<String, String> properties, int actual) {
        String declared = properties.get("dimension");
        if (declared != null && boundedInt(properties, "dimension", actual, 1, MAX_VECTOR_DIMENSION) != actual) {
            throw validation("Index dimension differs from the VECTOR column dimension");
        }
    }

    private static String normalizedAlgorithm(IndexDescriptor descriptor) {
        String defaultAlgorithm = descriptor.indexType() == IndexType.INVERTED
                ? "BM25" : "HNSW";
        return descriptor.properties().getOrDefault("algorithm", defaultAlgorithm)
                .trim().toUpperCase(Locale.ROOT);
    }

    private static String requireSingleColumn(IndexDescriptor descriptor) {
        if (descriptor.columnNames().size() != 1) {
            throw validation("Native index artifacts require exactly one source column");
        }
        return descriptor.columnNames().get(0);
    }

    private String stagingUri(String tenantId, IndexDescriptor descriptor) {
        return baseArtifactUri + "/v2/tenants/" + DeferredIndexAuthorityResolver.tenantScope(tenantId) + "/artifacts/"
                + safeComponent(descriptor.indexId()) + "/generation-"
                + descriptor.generation();
    }

    static String jobId(String tenantId, IndexDescriptor descriptor) {
        return "index-build:tenant-" + DeferredIndexAuthorityResolver.tenantScope(tenantId) + ":"
                + safeComponent(descriptor.indexId())
                + ":generation:" + descriptor.generation();
    }

    private static String safeComponent(String value) {
        if (value == null || value.isBlank() || value.contains("..")
                || value.contains("/") || value.contains("\\") || value.indexOf('\0') >= 0) {
            throw validation("Artifact path identity contains unsafe characters");
        }
        return value;
    }

    private static String stripTrailingSlash(String value) {
        String result = value;
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        if (result.isBlank()) {
            throw new IllegalArgumentException("baseArtifactUri must not be a filesystem root");
        }
        return result;
    }

    private static String requireTenantId(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            throw validation("tenant identity is required for durable artifact work");
        }
        return tenantId;
    }

    private static int boundedInt(
            Map<String, String> properties, String key,
            int defaultValue, int minimum, int maximum) {
        long value = boundedLong(properties, key, defaultValue, minimum, maximum);
        return Math.toIntExact(value);
    }

    private static long boundedLong(
            Map<String, String> properties, String key,
            long defaultValue, long minimum, long maximum) {
        String raw = properties.get(key);
        final long value;
        try {
            value = raw == null ? defaultValue : Long.parseLong(raw);
        } catch (NumberFormatException failure) {
            throw validation(key + " must be an integer", failure);
        }
        if (value < minimum || value > maximum) {
            throw validation(key + " must be in [" + minimum + ", " + maximum + "]");
        }
        return value;
    }

    private static double boundedDouble(
            Map<String, String> properties, String key,
            double defaultValue, double minimum, double maximum) {
        String raw = properties.get(key);
        final double value;
        try {
            value = raw == null ? defaultValue : Double.parseDouble(raw);
        } catch (NumberFormatException failure) {
            throw validation(key + " must be numeric", failure);
        }
        if (!Double.isFinite(value) || value < minimum || value > maximum) {
            throw validation(key + " must be finite and in [" + minimum + ", " + maximum + "]");
        }
        return value;
    }

    private static PangrovaException validation(String message) {
        return new PangrovaException(PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR, message);
    }

    private static PangrovaException validation(String message, Throwable cause) {
        return new PangrovaException(
                PangrovaErrorCode.METADATA_INDEX_VALIDATION_ERROR, message, cause);
    }

    private record VectorShape(int dimension, VectorType.ElementType elementType) {}

    private record NativeBuildSpec(
            DataBuildPlan plan,
            String artifactType,
            List<NamedValue> properties) {}
}
