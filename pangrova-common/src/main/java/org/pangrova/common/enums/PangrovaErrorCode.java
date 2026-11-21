package org.pangrova.common.enums;

/**
 * Categorized error codes for the Pangrova exception hierarchy.
 * Format: CATEGORY-NNNN.
 */
public enum PangrovaErrorCode {

    CONFIG_MISSING_KEY("CONFIG-0001", "Required configuration key is missing"),
    CONFIG_INVALID_VALUE("CONFIG-0002", "Configuration value is invalid"),

    VALIDATION_NULL_ARGUMENT("VALIDATION-0001", "Argument must not be null"),
    VALIDATION_INVALID_ARGUMENT("VALIDATION-0002", "Argument is invalid"),

    SPI_PROVIDER_NOT_FOUND("SPI-0001", "No SPI provider found"),
    SPI_MULTIPLE_PROVIDERS("SPI-0002", "Multiple SPI providers found"),

    PROTOCOL_SERIALIZATION_FAILED("PROTOCOL-0001", "Serialization failed"),
    PROTOCOL_DESERIALIZATION_FAILED("PROTOCOL-0002", "Deserialization failed"),

    PARSER_SYNTAX_ERROR("PARSER-0001", "SQL syntax error"),
    PARSER_UNSUPPORTED_FEATURE("PARSER-0002", "Unsupported SQL feature"),
    PARSER_INPUT_TOO_LARGE("PARSER-0003", "SQL input exceeds size or depth limit"),

    PLANNER_VALIDATION_FAILURE("PLANNER-0001", "Query validation failure"),
    PLANNER_OPTIMIZATION_FAILURE("PLANNER-0002", "Query optimization failure"),
    PLANNER_STATISTICS_UNAVAILABLE("PLANNER-0003", "Statistics unavailable, fallback to heuristic"),

    SUBSTRAIT_UNSUPPORTED_PLAN_NODE("SUBSTRAIT-0001", "Unsupported plan node type"),
    SUBSTRAIT_TYPE_MAPPING_FAILURE("SUBSTRAIT-0002", "Type mapping failure"),
    SUBSTRAIT_FUNCTION_MAPPING_FAILURE("SUBSTRAIT-0003", "Function mapping failure"),

    CACHE_L2_CONNECTION_FAILURE("CACHE-0001", "L2 Redis connection failure"),

    THROTTLE_GLOBAL_LIMIT("THROTTLE-0001", "Global concurrency limit exceeded"),
    THROTTLE_TENANT_LIMIT("THROTTLE-0002", "Tenant rate limit exceeded"),
    THROTTLE_USER_LIMIT("THROTTLE-0003", "User rate limit exceeded"),

    MATERIALIZED_VIEW_CREATION_FAILURE("MATVIEW-0001", "Materialized view creation failure"),
    MATERIALIZED_VIEW_REFRESH_FAILURE("MATVIEW-0002", "Materialized view refresh failure"),
    MATERIALIZED_VIEW_PERSISTENCE_FAILURE("MATVIEW-0003", "Materialized view metadata persistence failure"),
    MATERIALIZED_VIEW_RECOVERY_FAILURE("MATVIEW-0004", "Materialized view metadata recovery failure"),
    DYNAMIC_TABLE_NOT_READY("DYNATBL-0001", "Dynamic Table not ready for queries"),
    DYNAMIC_TABLE_NOT_FOUND("DYNATBL-0002", "Dynamic Table not found"),
    DYNAMIC_TABLE_CREATE_FAILED("DYNATBL-0003", "Dynamic Table creation failed"),
    DYNAMIC_TABLE_ALTER_INVALID("DYNATBL-0004", "Dynamic Table alter operation invalid"),
    DYNAMIC_TABLE_RECOVERY_TIMEOUT("DYNATBL-0005", "Dynamic Table recovery timeout"),
    DYNAMIC_TABLE_EPOCH_STALE("DYNATBL-0006", "Dynamic Table epoch fencing rejected stale write"),

    MATERIALIZATION_OBJECT_NOT_FOUND("MATERIALIZATION-0001", "Materialization object not found"),
    MATERIALIZATION_INVALID_STATE_TRANSITION(
            "MATERIALIZATION-0002", "Materialization state transition is not allowed"),
    MATERIALIZATION_DEFINITION_INVALID("MATERIALIZATION-0003", "Materialization definition is invalid"),
    MATERIALIZATION_FENCE_REJECTED("MATERIALIZATION-0004", "Materialization execution fence is stale"),
    MATERIALIZATION_CONCURRENT_PUBLICATION(
            "MATERIALIZATION-0005", "Materialization publication lost the head CAS"),
    MATERIALIZATION_EVIDENCE_MISMATCH(
            "MATERIALIZATION-0006", "Materialization commit evidence does not match the candidate"),
    MATERIALIZATION_CAPABILITY_UNAVAILABLE(
            "MATERIALIZATION-0007", "Required materialization capability is unavailable"),
    MATERIALIZATION_INCREMENTAL_UNSUPPORTED(
            "MATERIALIZATION-0008", "No compatible incremental strategy for the definition and sources"),
    MATERIALIZATION_RESERVED_NAMESPACE(
            "MATERIALIZATION-0009", "Access to the internal materialization namespace is forbidden"),
    MATERIALIZATION_REWRITE_UNSAFE("MATERIALIZATION-0010", "Materialized view rewrite is not safe"),
    MATERIALIZATION_POLICY_DRIFT(
            "MATERIALIZATION-0011", "Source policy changed during materialization"),
    MATERIALIZATION_REPOSITORY_FAILURE(
            "MATERIALIZATION-0012", "Materialization repository operation failed"),
    MATERIALIZATION_WRITE_CAPABILITY_INVALID(
            "MATERIALIZATION-0013", "Materialization write capability is invalid or expired"),
    MATERIALIZATION_LEASE_EXPIRED("MATERIALIZATION-0014", "Materialization schedule lease expired"),
    MATERIALIZATION_DEPENDENCY_CONFLICT(
            "MATERIALIZATION-0015", "Materialization dependency graph conflict"),
    MATERIALIZATION_REQUEST_REJECTED(
            "MATERIALIZATION-0016", "Materialization request was rejected"),

    STATS_COLLECTION_TIMEOUT("STATS-0001", "Statistics collection timeout"),

    NATIVE_JNI_EXCEPTION("NATIVE-0001", "C++ exception during JNI call"),
    NATIVE_INVALID_HANDLE("NATIVE-0002", "Invalid executor handle"),
    NATIVE_DATA_INTERFACE_ERROR("NATIVE-0003", "Native columnar data interface error"),

    METADATA_CATALOG_NOT_FOUND("METADATA-0001", "Catalog not found"),
    METADATA_CATALOG_ALREADY_EXISTS("METADATA-0002", "Catalog already exists"),
    METADATA_CATALOG_SUSPENDED("METADATA-0003", "Catalog is suspended"),
    METADATA_CATALOG_PROVIDER_NOT_FOUND("METADATA-0004", "Catalog provider not found"),
    METADATA_CATALOG_BACKEND_ERROR("METADATA-0005", "Catalog backend communication failure"),
    METADATA_METASTORE_ERROR("METADATA-0006", "Metastore persistence failure"),
    METADATA_SERIALIZATION_ERROR("METADATA-0007", "Metadata serialization/deserialization failure"),
    METADATA_TABLE_NOT_FOUND("METADATA-0008", "Table not found"),
    METADATA_SYNC_FAILED("METADATA-0009", "Metadata sync failed after retries exhausted"),
    METADATA_RESOLUTION_FAILED("METADATA-0010", "Metadata resolution failed for catalog/schema"),
    METADATA_INDEX_ALREADY_EXISTS("METADATA-0011", "Index already exists"),
    METADATA_INDEX_NOT_FOUND("METADATA-0012", "Index not found"),
    METADATA_INDEX_VALIDATION_ERROR("METADATA-0013", "Index validation error"),
    METADATA_STALE_EPOCH("METADATA-0014", "Conditional update failed due to stale epoch"),
    METADATA_SCHEMA_INCOMPATIBLE("METADATA-0015", "Schema changes incompatible"),
    METADATA_CHANGED_DURING_PLANNING("METADATA-0016", "Metadata changed during query planning"),
    METADATA_CATALOG_ACCESS_DENIED("METADATA-0017", "Tenant not authorized to access catalog"),
    METADATA_DEPENDENCY_VIOLATION("METADATA-0018", "Dependency violation"),
    METADATA_FILE_SPACE_NOT_FOUND("METADATA-0019", "Volume or stage not found"),
    METADATA_FILE_SPACE_ALREADY_EXISTS("METADATA-0020", "Volume or stage already exists"),

    TABLE_CONTROL_PERSISTENCE_FAILED("TABLE-0001", "Table control persistence failed"),
    TABLE_CONTROL_STALE_WRITE("TABLE-0002", "Table control write rejected by optimistic concurrency"),

    FILE_OBJECT_NOT_FOUND("FILE-0001", "File object not found"),
    FILE_OBJECT_PRECONDITION_FAILED("FILE-0002", "File object version does not match the request"),
    FILE_PATH_INVALID("FILE-0003", "File path is not valid for this volume or stage"),
    FILE_UPLOAD_STATE_INVALID("FILE-0004", "Upload session state rejected the request"),
    FILE_STORAGE_UNAVAILABLE("FILE-0005", "Object storage is unavailable"),
    FILE_STORAGE_NOT_CONFIGURED("FILE-0006", "Object storage is not configured for this space"),
    FILE_LIMIT_EXCEEDED("FILE-0007", "File operation exceeds a configured limit"),

    RUNTIME_TASK_FAILED("RUNTIME-0001", "Task execution failed"),
    RUNTIME_STAGE_FAILED("RUNTIME-0002", "Stage execution failed"),
    RUNTIME_QUERY_TIMEOUT("RUNTIME-0003", "Query execution timeout"),
    RUNTIME_RESOURCE_EXHAUSTED("RUNTIME-0004", "Resource exhausted"),

    COORDINATOR_NO_AVAILABLE_WORKER("COORDINATOR-0001", "No available worker for task dispatch"),
    COORDINATOR_QUERY_REJECTED("COORDINATOR-0002", "Query rejected, coordinator is draining"),
    COORDINATOR_DISPATCH_FAILED("COORDINATOR-0003", "Task dispatch to worker failed"),
    COORDINATOR_DISPATCH_REJECTED("COORDINATOR-0004", "Worker rejected dispatched task"),
    COORDINATOR_CIRCUIT_BREAKER_OPEN("COORDINATOR-0005", "Circuit breaker open for worker"),
    COORDINATOR_DISPATCH_INTERRUPTED("COORDINATOR-0006", "Task dispatch interrupted"),
    COORDINATOR_MEMORY_EXCEEDED("COORDINATOR-0007", "Coordinator TypedRow allocation exceeded limit"),
    COORDINATOR_DDL_NOT_ON_LEADER("COORDINATOR-0008", "DDL must execute on leader coordinator"),

    CONNECTOR_READ_ERROR("CONNECTOR-0001", "Connector data read failed"),
    CONNECTOR_WRITE_ERROR("CONNECTOR-0002", "Connector data write failed"),
    CONNECTOR_TABLE_NOT_FOUND("CONNECTOR-0003", "Connector table not found"),
    CONNECTOR_TABLE_ALREADY_EXISTS("CONNECTOR-0004", "Connector table already exists"),
    CONNECTOR_SCHEMA_NOT_FOUND("CONNECTOR-0005", "Connector schema/database not found"),
    CONNECTOR_DDL_FAILED("CONNECTOR-0006", "Connector DDL operation failed"),
    CONNECTOR_SPLIT_FETCH_FAILED("CONNECTOR-0007", "Connector split enumeration failed"),
    CONNECTOR_SPLIT_FETCH_TIMEOUT("CONNECTOR-0008", "Connector split enumeration timed out"),
    CONNECTOR_SPLIT_LIMIT_REACHED("CONNECTOR-0009", "Connector split enumeration exceeded limit"),
    CONNECTOR_PERMISSION_DENIED("CONNECTOR-0010", "Connector permission denied"),
    CONNECTOR_TRANSIENT_ERROR("CONNECTOR-0011", "Connector transient error, retry may succeed"),
    CONNECTOR_CONFIGURATION_ERROR("CONNECTOR-0012", "Connector configuration invalid"),
    CONNECTOR_SNAPSHOT_NOT_FOUND("CONNECTOR-0025", "Snapshot not found"),
    CONNECTOR_TAG_NOT_FOUND("CONNECTOR-0026", "Tag not found"),
    CONNECTOR_UNSUPPORTED_OPERATION("CONNECTOR-0027", "Connector unsupported operation"),

    EXCHANGE_SOURCE_UNAVAILABLE("EXCHANGE-0001", "Exchange source unavailable"),
    EXCHANGE_DATA_CORRUPTION("EXCHANGE-0002", "Exchange data corruption"),
    EXCHANGE_CONNECTION_FAILED("EXCHANGE-0003", "Exchange source connection failed"),
    EXCHANGE_FETCH_FAILED("EXCHANGE-0004", "Shuffle data unavailable from upstream stage"),

    RESOURCE_MEMORY_EXCEEDED("RESOURCE-0001", "Memory quota exceeded"),
    RESOURCE_SPILL_FAILED("RESOURCE-0002", "Spill to disk failed"),

    TRANSPORT_CONNECTION_FAILED("TRANSPORT-0001", "Transport connection failed"),
    TRANSPORT_STREAM_ERROR("TRANSPORT-0002", "Transport stream error"),
    TRANSPORT_RESULT_TOO_LARGE("TRANSPORT-0003", "Transport result stream exceeded size limit"),

    NATIVE_INIT_FAILED("NATIVE-0004", "Native engine initialization failed"),
    NATIVE_LIBRARY_UNAVAILABLE("NATIVE-0005", "Native library not available"),
    NATIVE_FALLBACK_REQUIRED("NATIVE-0006", "Native connector encountered unsupported feature, fallback required"),
    NATIVE_UNSUPPORTED_PLAN("NATIVE-0007", "Plan contains operators unsupported by native engine"),

    FUNCTION_NOT_FOUND("FUNCTION-0001", "User-defined function not found in registry"),

    AUTHENTICATION_FAILED("SECURITY-0001", "Authentication failed"),
    ACCESS_DENIED("SECURITY-0002", "Access denied"),
    ROW_LEVEL_SECURITY_POLICY_ERROR("SECURITY-0003", "Row-level security policy error"),
    COLUMN_MASK_ERROR("SECURITY-0004", "Column masking policy error"),
    AUDIT_WRITE_FAILED("SECURITY-0005", "Audit event write failed"),
    TOKEN_EXPIRED("SECURITY-0006", "Authentication token expired"),
    ENCRYPTION_FAILED("SECURITY-0007", "Encryption or decryption operation failed"),
    TENANT_QUOTA_EXCEEDED("TENANT-0001", "Tenant quota exceeded"),
    TENANT_OWNERSHIP_MISMATCH("TENANT-0002", "Tenant ownership mismatch"),
    TENANT_NOT_FOUND("TENANT-0003", "Tenant not found"),
    TENANT_INVALID_STATE("TENANT-0004", "Tenant is in an invalid state for the requested operation"),

    DML_CONCURRENCY_LIMIT("DML-0001", "DML concurrency limit exceeded"),
    DML_CONFLICT_DETECTED("DML-0002", "DML write conflict detected"),
    DML_SINK_LIFECYCLE_ERROR("DML-0003", "DML sink lifecycle error"),
    DML_SOURCE_TOO_LARGE("DML-0004", "MERGE source table exceeds row limit"),

    INDEX_BUILD_FAILED("INDEX-0001", "Index build failed"),
    INDEX_EVAL_CIRCUIT_OPEN("INDEX-0002", "Index evaluation circuit breaker open"),
    INDEX_GC_CONFLICT("INDEX-0003", "Index GC conditional removal conflict"),
    INDEX_SERDE_VERSION_MISMATCH("INDEX-0004", "Index serde version mismatch"),

    CLUSTERING_OVERLAP_CALCULATION_FAILED("CLUSTER-0001", "Clustering overlap calculation failed"),
    CLUSTERING_RECLUSTER_FAILED("CLUSTER-0002", "Recluster operation failed"),

    RESILIENCE_CIRCUIT_OPEN("RESILIENCE-0001", "Circuit breaker open"),
    RESILIENCE_RETRY_EXHAUSTED("RESILIENCE-0002", "Retry attempts exhausted"),
    RESILIENCE_CHECKPOINT_FAILED("RESILIENCE-0003", "Checkpoint persistence failed"),

    STREAMING_CDC_PARSE_ERROR("STREAMING-0001", "CDC message parse error"),
    STREAMING_CDC_QUOTA_EXCEEDED("STREAMING-0002", "CDC quota exceeded"),
    STREAMING_MAINTENANCE_TIMEOUT("STREAMING-0003", "Maintenance task timeout"),
    STREAMING_WATERMARK_FAILED("STREAMING-0004", "Watermark tracker failed"),
    STREAMING_CDC_STALE_FENCE("STREAMING-0005", "CDC pipeline fencing token is stale"),
    STREAMING_STORE_ERROR("STREAMING-0006", "Streaming state store persistence error"),

    STREAM_SOURCE_UNAVAILABLE("STREAM-0001", "Stream source is unavailable"),
    STREAM_NOT_FOUND("STREAM-0002", "Stream not found"),
    STREAM_ALREADY_EXISTS("STREAM-0003", "Stream already exists"),
    STREAM_QUOTA_EXCEEDED("STREAM-0004", "Stream quota exceeded"),

    SEMANTIC_STORE_ERROR("SEMANTIC-0001", "Semantic View store persistence failure"),

    GOVERNANCE_LINEAGE_EMISSION_FAILED("GOVERNANCE-0001", "Lineage event emission failed"),
    GOVERNANCE_QUALITY_CHECK_FAILED("GOVERNANCE-0002", "Data quality check failed"),

    OBS_SDK_INIT_FAILED("OBS-0001", "OpenTelemetry SDK initialization failed"),
    OBS_TRACE_PROPAGATION_FAILED("OBS-0002", "Trace context propagation failed"),
    OBS_NATIVE_METRICS_TIMEOUT("OBS-0003", "Native metrics pull timeout"),
    OBS_NATIVE_METRICS_STALE("OBS-0004", "Native metrics stale"),
    OBS_AUDIT_PERSISTENCE_FAILED("OBS-0005", "Audit record persistence failed"),

    OPS_UPGRADE_DRAIN_TIMEOUT("OPS-0001", "Rolling upgrade drain timeout"),
    OPS_UPGRADE_DEPLOY_FAILED("OPS-0002", "Rolling upgrade deploy failed"),
    OPS_VERSION_INCOMPATIBLE("OPS-0003", "Version incompatible"),
    OPS_DDL_EXECUTION_FAILED("OPS-0004", "DDL job execution failed"),
    OPS_FAILOVER_VERIFICATION_FAILED("OPS-0005", "Failover verification failed"),
    OPS_SCALING_EXECUTION_FAILED("OPS-0006", "Auto-scaling execution failed"),
    OPS_CONFIG_RELOAD_FAILED("OPS-0007", "Config hot reload failed"),
    OPS_PLAINTEXT_CREDENTIAL("OPS-0008", "Plaintext credential detected"),
    OPS_BOOTSTRAP_PHASE_FAILED("OPS-0009", "Bootstrap phase failed"),

    WORKER_REGISTRATION_SERIALIZATION_FAILED("WORKER-0001", "Worker registration serialization failed"),
    WORKER_LOST("WORKER-0002", "Worker declared dead while executing tasks"),

    AI_EMBEDDING_MODEL_NOT_FOUND("AI-0001", "Embedding model not found"),
    AI_EMBEDDING_MODEL_ALREADY_EXISTS("AI-0002", "Embedding model already exists"),
    AI_EMBEDDING_DIMENSION_MISMATCH("AI-0003", "Model dimension does not match column dimension"),
    AI_EMBEDDING_MODEL_IN_USE("AI-0004", "Embedding model is referenced by dependent columns"),
    AI_EMBEDDING_VERSION_CONFLICT("AI-0005", "Optimistic concurrency conflict on model version"),
    AI_EMBEDDING_ACCESS_DENIED("AI-0006", "Insufficient privilege for embedding model operation"),
    AI_EMBEDDING_MODEL_DROP_UNAVAILABLE(
            "AI-0007", "Embedding model drop requires durable dependency coordination"),
    AI_CONTROL_STORE_ERROR("AI-0008", "AI control-plane store persistence failure"),
    AI_EMBEDDING_PROVIDER_UNAVAILABLE("AI-3003", "Embedding provider unavailable"),
    AI_BRANCH_NOT_FOUND("AI-3008", "Branch does not exist"),
    AI_BRANCH_ALREADY_EXISTS("AI-3011", "Branch already exists"),
    AI_BRANCH_SNAPSHOT_EXPIRED("AI-3007", "Branch references an expired snapshot"),
    AI_MERGE_NOT_SUPPORTED("AI-3012", "MERGE BRANCH is not supported"),
    AI_BLOB_SIZE_EXCEEDED("AI-3010", "BLOB content exceeds maximum allowed size"),
    AI_EMBEDDING_QUEUE_FULL("AI-3014", "Embedding queue backpressure active: ASYNC INSERT blocked until queue drains"),
    AI_INFERENCE_PROVIDER_UNAVAILABLE("AI-3015", "Inference provider unavailable"),
    AI_MODEL_SERVICE_UNAVAILABLE("AI-3016", "Model service unavailable on data plane"),
    AI_INVOCATION_AUTHORIZATION_DENIED(
            "AI-3017", "AI invocation authorization denied"),

    TEST_REFERENCE_ENGINE_INIT_FAILED("TEST-0001", "Reference engine initialization failed"),
    TEST_RESULT_MISMATCH("TEST-0002", "Result mismatch detected"),
    TEST_PERFORMANCE_REGRESSION("TEST-0003", "Performance regression detected");

    private final String code;
    private final String description;

    PangrovaErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
