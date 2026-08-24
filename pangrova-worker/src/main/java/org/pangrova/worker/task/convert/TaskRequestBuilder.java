package org.pangrova.worker.task.convert;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v1.ShuffleSerdeFormat;
import org.pangrova.protocol.v1.SplitInfo;
import org.pangrova.protocol.v1.TaskRequest;
import org.pangrova.worker.execution.filter.ConstraintBasedSplitFilter;
import org.pangrova.worker.execution.filter.DynamicFilterApplicator;
import org.pangrova.worker.execution.filter.DynamicFilterApplicator.FilterResult;
import org.pangrova.worker.execution.filter.DynamicPartitionFilterMetrics;
import org.pangrova.worker.task.TaskTypeMapper;
import org.pangrova.worker.task.index.IndexEvalSplitFilter;
import org.pangrova.worker.task.index.NativeIndexHintConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Converts REST SubmitTaskRequest DTO into TaskRequest protobuf bytes.
 *
 * <p>Splits are deserialized from Base64-encoded SplitInfo proto bytes and placed
 * into TaskRequest.splits (field 4) as typed SplitInfo messages. Field 9 is
 * reserved (previously java_split_bytes, now removed).</p>
 */
public final class TaskRequestBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(TaskRequestBuilder.class);

    private static final DynamicFilterApplicator FILTER_APPLICATOR = new DynamicFilterApplicator();

    /** Lazy-initialized worker-side L2 index evaluator (null if indexing jar absent). */
    private static volatile IndexEvalSplitFilter indexEvalFilter;
    private static volatile boolean indexEvalFilterInitialized;

    private TaskRequestBuilder() {}

    /**
     * Build TaskRequest protobuf bytes from a REST task submission request.
     *
     * @param request the REST task submission request
     * @return serialized TaskRequest protobuf bytes
     */
    public static byte[] build(SubmitTaskRequest request) {
        checkNotNull(request, "request must not be null");
        checkNotNull(request.planFragment(), "planFragment must not be null");

        TaskRequest.Builder builder = TaskRequest.newBuilder();
        mapIdentifiers(builder, request);
        mapPlanAndSplits(builder, request);
        mapSessionProperties(builder, request);
        mapResourceLimit(builder, request);
        mapWriteConfig(builder, request);
        mapShuffleConfig(builder, request);

        return builder.build().toByteArray();
    }

    private static void mapIdentifiers(TaskRequest.Builder builder, SubmitTaskRequest request) {
        builder.setTaskId(nullToEmpty(request.taskId()));
        builder.setQueryId(nullToEmpty(request.queryId()));
        builder.setStageId(request.stageId());
        builder.setPartitionId(request.partitionId());
        builder.setTaskType(TaskTypeMapper.toProtoType(request.taskType()));
        builder.setTenantId(nullToEmpty(request.tenantId()));
        builder.setPrincipal(nullToEmpty(request.principal()));
    }

    private static void mapPlanAndSplits(TaskRequest.Builder builder, SubmitTaskRequest request) {
        byte[] planBytes = Base64.getDecoder().decode(request.planFragment());
        builder.setPlanFragment(ByteString.copyFrom(planBytes));

        // P1-CHAIN-6: Decode splits once, reuse for both DPP filtering and proto building.
        // DynamicFilterApplicator still operates on Base64 strings (its API contract),
        // but we decode the filtered result only once into SplitInfo messages.
        List<String> effectiveSplits = applyDynamicFilters(request);
        List<SplitInfo> splitInfos = decodeSplitInfos(effectiveSplits);

        // Constraint-based split skipping: use split_constraint (partition values + column
        // statistics) to skip splits whose data range doesn't overlap with query predicates.
        splitInfos = applyConstraintFilter(splitInfos, request);

        // L2 index evaluation: filter splits using index hints from coordinator
        splitInfos = applyIndexEvalFilter(splitInfos);

        // Validate and log index hints for native engine page-level skipping (R11.3)
        NativeIndexHintConsumer.validateHintsForNativeEngine(
                splitInfos, request.queryId(), request.stageId());

        validateTableNamePresent(splitInfos, request.queryId(), request.stageId());
        builder.addAllSplits(splitInfos);
    }

    /**
     * Applies DPP dynamic filters to splits before native execution.
     * If no filters are present, returns the original splits unchanged.
     */
    private static List<String> applyDynamicFilters(SubmitTaskRequest request) {
        List<DynamicFilterPayload> filters = request.dynamicFilters();
        List<String> splits = request.splits();
        if (filters == null || filters.isEmpty()) {
            return splits;
        }
        FilterResult result = FILTER_APPLICATOR.apply(
                splits, filters, request.stageId(), request.queryId());
        DynamicPartitionFilterMetrics.recordSplitFiltering(
                request.queryId(), request.stageId(),
                result.totalSplits(), result.filteredSplits());
        return result.retainedSplits();
    }

    /**
     * Applies constraint-based split filtering using split_constraint metadata.
     * Evaluates dynamic filter predicates against partition values and column
     * statistics carried in each split's SplitConstraintInfo.
     */
    private static List<SplitInfo> applyConstraintFilter(
            List<SplitInfo> splits, SubmitTaskRequest request) {
        List<DynamicFilterPayload> filters = request.dynamicFilters();
        if (filters == null || filters.isEmpty() || splits.isEmpty()) {
            return splits;
        }
        ConstraintBasedSplitFilter.FilterResult result =
                ConstraintBasedSplitFilter.filter(splits, filters, request.queryId(), request.stageId());
        return result.retainedSplits();
    }

    /**
     * Applies worker-side L2 index evaluation to filter splits.
     * Lazy-initializes the evaluator via ServiceLoader on first call.
     * If no evaluator is available, returns splits unchanged.
     */
    private static List<SplitInfo> applyIndexEvalFilter(List<SplitInfo> splits) {
        if (!indexEvalFilterInitialized) {
            synchronized (TaskRequestBuilder.class) {
                if (!indexEvalFilterInitialized) {
                    indexEvalFilter = IndexEvalSplitFilter.tryCreate();
                    indexEvalFilterInitialized = true;
                }
            }
        }
        if (indexEvalFilter == null) {
            LOG.warn("event=index_eval_filter_unavailable reason=no_evaluator_on_classpath"
                    + " impact=l2_index_pruning_disabled");
            return splits;
        }
        return indexEvalFilter.filterSplits(splits);
    }

    private static void mapSessionProperties(TaskRequest.Builder builder, SubmitTaskRequest request) {
        Map<String, String> props = request.sessionProperties();
        if (props != null && !props.isEmpty()) {
            builder.putAllSessionProperties(props);
        }
    }

    private static void mapResourceLimit(TaskRequest.Builder builder, SubmitTaskRequest request) {
        org.pangrova.protocol.rest.config.ResourceLimitPayload dto = request.resourceLimit();
        if (dto == null) {
            return;
        }
        org.pangrova.protocol.v1.ResourceLimit limit = org.pangrova.protocol.v1.ResourceLimit.newBuilder()
                .setMaxMemoryBytes(dto.maxMemoryBytes())
                .setMaxThreads(dto.maxThreads())
                .setMaxSpillBytes(dto.maxSpillBytes())
                .setSpillDirectory(nullToEmpty(dto.spillDirectory()))
                .build();
        builder.setResourceLimit(limit);
    }

    private static void mapWriteConfig(TaskRequest.Builder builder, SubmitTaskRequest request) {
        org.pangrova.protocol.rest.config.WriteConfig dto = request.writeConfig();
        if (dto == null) {
            return;
        }
        org.pangrova.protocol.v1.WriteConfig.Builder wcBuilder = org.pangrova.protocol.v1.WriteConfig.newBuilder()
                .setTargetTable(nullToEmpty(dto.targetTable()))
                .setWriteMode(nullToEmpty(dto.writeMode()))
                .setFileFormat(mapFileFormat(dto.fileFormat()))
                .setTargetFileSize(dto.targetFileSize());
        if (dto.partitionSpec() != null) {
            wcBuilder.addAllPartitionSpec(dto.partitionSpec());
        }
        builder.setWriteConfig(wcBuilder.build());
    }

    private static org.pangrova.protocol.v1.FileFormat mapFileFormat(String format) {
        if (format == null || format.isEmpty()) {
            return org.pangrova.protocol.v1.FileFormat.FILE_FORMAT_PARQUET;
        }
        return switch (format.toUpperCase()) {
            case "ORC" -> org.pangrova.protocol.v1.FileFormat.FILE_FORMAT_ORC;
            default -> org.pangrova.protocol.v1.FileFormat.FILE_FORMAT_PARQUET;
        };
    }

    private static void mapShuffleConfig(TaskRequest.Builder builder, SubmitTaskRequest request) {
        org.pangrova.protocol.rest.config.ShuffleConfig dto = request.shuffleConfig();
        if (dto == null) {
            return;
        }
        var protoBuilder = org.pangrova.protocol.v1.ShuffleConfig.newBuilder()
                .setSerdeFormat(mapShuffleSerdeFormat(dto.serdeFormat()))
                .setCompressionCodec(nullToEmpty(dto.compressionCodec()))
                .setBatchSize(dto.batchSize())
                .setNumPartitions(dto.numPartitions());

        applyPartitionKeys(protoBuilder, dto);
        applyPersistenceMode(protoBuilder, dto);
        builder.setShuffleConfig(protoBuilder.build());
    }

    private static void applyPartitionKeys(
            org.pangrova.protocol.v1.ShuffleConfig.Builder protoBuilder,
            org.pangrova.protocol.rest.config.ShuffleConfig dto) {
        if (dto.partitionKeyIndices() != null) {
            dto.partitionKeyIndices().forEach(protoBuilder::addPartitionKeyIndices);
        }
    }

    private static void applyPersistenceMode(
            org.pangrova.protocol.v1.ShuffleConfig.Builder protoBuilder,
            org.pangrova.protocol.rest.config.ShuffleConfig dto) {
        if (dto.persistenceMode() == null) {
            return;
        }
        switch (dto.persistenceMode()) {
            case "MEMORY_AND_DISK" -> protoBuilder.setPersistenceMode(
                    org.pangrova.protocol.v1.ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MEMORY_AND_DISK);
            case "MEMORY_AND_REMOTE" -> protoBuilder.setPersistenceMode(
                    org.pangrova.protocol.v1.ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MEMORY_AND_REMOTE);
            default -> protoBuilder.setPersistenceMode(
                    org.pangrova.protocol.v1.ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MEMORY_ONLY);
        }
    }

    /**
     * Decode Base64-encoded SplitInfo proto bytes into SplitInfo messages.
     *
     * @param encodedSplits Base64-encoded SplitInfo proto bytes
     * @return list of decoded SplitInfo messages
     */
    public static List<SplitInfo> decodeSplitInfos(List<String> encodedSplits) {
        if (encodedSplits == null || encodedSplits.isEmpty()) {
            return List.of();
        }
        return encodedSplits.stream()
                .map(TaskRequestBuilder::parseSplitInfo)
                .toList();
    }

    private static SplitInfo parseSplitInfo(String encoded) {
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            return SplitInfo.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to parse SplitInfo from Base64 split", e);
        }
    }

    /**
     * Validates that splits carry table_name in multi-table plans.
     *
     * <p>Multi-table is detected by 2+ distinct table_name values among splits,
     * or by 2+ distinct connector_ids. When multiple tables are involved and any
     * split lacks table_name, native split routing cannot determine which scan
     * node owns which split — fail closed instead of falling back to broadcast.</p>
     */
    private static void validateTableNamePresent(List<SplitInfo> splits, String queryId, int stageId) {
        if (splits.size() <= 1) {
            return;
        }
        long distinctTableNames = countDistinctTableNames(splits);
        long distinctConnectors = countDistinctConnectors(splits);
        if (distinctTableNames <= 1 && distinctConnectors <= 1) {
            return;
        }
        checkNoMissingTableNames(splits, queryId, stageId, distinctTableNames, distinctConnectors);
    }

    private static long countDistinctTableNames(List<SplitInfo> splits) {
        return splits.stream()
                .map(SplitInfo::getTableName)
                .filter(name -> !name.isEmpty())
                .distinct()
                .count();
    }

    private static long countDistinctConnectors(List<SplitInfo> splits) {
        return splits.stream()
                .map(SplitInfo::getConnectorId)
                .distinct()
                .count();
    }

    private static void checkNoMissingTableNames(List<SplitInfo> splits, String queryId,
                                                  int stageId, long distinctTableNames,
                                                  long distinctConnectors) {
        boolean anyMissing = splits.stream().anyMatch(s -> s.getTableName().isEmpty());
        if (anyMissing) {
            throw new IllegalStateException(
                    "Multi-table plan splits lack table_name, native split routing would fail. "
                            + "queryId=" + queryId + " stageId=" + stageId
                            + " splitCount=" + splits.size()
                            + " distinctTableNames=" + distinctTableNames
                            + " distinctConnectors=" + distinctConnectors);
        }
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static ShuffleSerdeFormat mapShuffleSerdeFormat(String format) {
        if (format == null) {
            return ShuffleSerdeFormat.SHUFFLE_SERDE_PRESTO_PAGES;
        }
        return switch (format) {
            case "PRESTO_PAGES" -> ShuffleSerdeFormat.SHUFFLE_SERDE_PRESTO_PAGES;
            default -> ShuffleSerdeFormat.SHUFFLE_SERDE_PRESTO_PAGES;
        };
    }
}
