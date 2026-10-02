package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.Map;
import java.util.Objects;

import com.google.protobuf.ByteString;
import org.pangrova.protocol.rest.config.ShuffleConfig;
import org.pangrova.protocol.rest.config.WriteConfig;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.ShuffleExecutionBinding;
import org.pangrova.protocol.v2.ShufflePersistenceMode;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.protocol.v2.ShuffleTaskOptions;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.WriteTaskOptions;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.shuffle.ShuffleHandle;

/** Builds canonical execution options for a worker task. */
final class TaskExecutionOptionsFactory {

    private static final String RESERVED_DEADLINE_SESSION_PROPERTY =
            "pangrova.deadline_epoch_ms";

    private TaskExecutionOptionsFactory() {}

    static TaskExecutionOptions build(
            Map<String, String> sessionProperties,
            WriteConfig writeConfig,
            ShuffleConfig shuffleConfig,
            boolean directResultOutput,
            ShuffleHandle shuffleHandle,
            long planEpoch) {
        TaskExecutionOptions.Builder options = TaskExecutionOptions.newBuilder()
                .putAllSessionProperties(validatedSessionProperties(sessionProperties))
                .setDirectResultOutput(directResultOutput);
        if (writeConfig != null) {
            options.setWrite(writeOptions(writeConfig));
        }
        if (shuffleConfig != null) {
            options.setShuffle(shuffleOptions(shuffleConfig, shuffleHandle, planEpoch));
        }
        return options.build();
    }

    private static WriteTaskOptions writeOptions(WriteConfig config) {
        return WriteTaskOptions.newBuilder()
                .setTargetTable(Objects.requireNonNullElse(config.targetTable(), ""))
                .setWriteMode(Objects.requireNonNullElse(config.writeMode(), ""))
                .addAllPartitionSpec(config.partitionSpec())
                .setFileFormat(fileFormat(config.fileFormat()))
                .setTargetFileSize(config.targetFileSize())
                .setOutputBinding(decodeOutputBinding(config.outputBindingBase64()))
                .build();
    }

    private static WriteOutputBinding decodeOutputBinding(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            throw new IllegalArgumentException(
                    "Write task requires a Coordinator-allocated output binding");
        }
        try {
            WriteOutputBinding binding = WriteOutputBinding.parseFrom(
                    java.util.Base64.getDecoder().decode(encoded));
            if (binding.getBindingCase() == WriteOutputBinding.BindingCase.BINDING_NOT_SET) {
                throw new IllegalArgumentException("Write output binding is empty");
            }
            return binding;
        } catch (com.google.protobuf.InvalidProtocolBufferException
                | IllegalArgumentException failure) {
            throw new IllegalArgumentException("Invalid write output binding", failure);
        }
    }

    private static FileFormat fileFormat(String format) {
        if (format == null || format.isBlank()
                || "PARQUET".equalsIgnoreCase(format)) {
            return FileFormat.FILE_FORMAT_PARQUET;
        }
        return switch (format.toUpperCase(java.util.Locale.ROOT)) {
            case "ORC" -> FileFormat.FILE_FORMAT_ORC;
            case "LANCE" -> FileFormat.FILE_FORMAT_LANCE;
            default -> throw new IllegalArgumentException(
                    "Unsupported write file format: " + format);
        };
    }

    private static ShuffleTaskOptions shuffleOptions(
            ShuffleConfig config, ShuffleHandle shuffleHandle, long planEpoch) {
        if (config.numPartitions() < 0) {
            throw new IllegalArgumentException(
                    "shuffle partition count must be non-negative");
        }
        ShufflePersistenceMode persistenceMode = persistenceMode(
                config.persistenceMode());
        ShuffleTaskOptions.Builder options = ShuffleTaskOptions.newBuilder()
                .setSerde(batchSerde(config.serdeFormat()))
                .setCompressionCodec(config.compressionCodec())
                .setBatchSize(config.batchSize())
                .setPersistenceMode(persistenceMode)
                .setPartitionCount(config.numPartitions())
                .addAllPartitionKeyIndices(config.partitionKeyIndices());
        if (persistenceMode
                == ShufflePersistenceMode.SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE) {
            options.setExecutionBinding(remoteBinding(
                    shuffleHandle, planEpoch, config.numPartitions()));
        }
        return options.build();
    }

    private static ShuffleExecutionBinding remoteBinding(
            ShuffleHandle shuffleHandle, long planEpoch, int partitionCount) {
        if (shuffleHandle == null || shuffleHandle.endpoints().isEmpty()
                || planEpoch <= 0) {
            throw new IllegalStateException(
                    "Remote shuffle requires a registered execution binding");
        }
        shuffleHandle.validatePartitionCoverage(partitionCount);
        // This Coordinator-held stage ticket is replaced with an exact task
        // grant after control identity binding and before Worker dispatch.
        ShuffleExecutionBinding.Builder binding = ShuffleExecutionBinding.newBuilder()
                .setShuffleId(shuffleHandle.shuffleId())
                .setPlanEpoch(planEpoch)
                .setCommitAddress(org.pangrova.protocol.v2.ShuffleServiceAddress
                        .newBuilder()
                        .setHost(shuffleHandle.commitAddress().host())
                        .setPort(shuffleHandle.commitAddress().port())
                        .setTlsIdentity(shuffleHandle.commitAddress().tlsIdentity()))
                .setWriteCapability(org.pangrova.protocol.v2.ShuffleAccessCapability
                        .newBuilder()
                        .setToken(ByteString.copyFrom(
                                shuffleHandle.writeAccess().token()))
                        .setExpiresAtEpochMs(
                                shuffleHandle.writeAccess().expiresAtEpochMs()));
        shuffleHandle.endpoints().forEach(endpoint ->
                binding.addEndpoints(ShuffleServiceEndpoint.newBuilder()
                        .setHost(endpoint.host())
                        .setPort(endpoint.port())
                        .setPartitionStart(endpoint.partitionStart())
                        .setPartitionEnd(endpoint.partitionEnd())
                        .setTlsIdentity(endpoint.tlsIdentity())));
        return binding.build();
    }

    private static BatchSerde batchSerde(String serde) {
        return switch (serde) {
            case "NATIVE_ROW", "PRESTO_PAGES" -> BatchSerde.BATCH_SERDE_NATIVE_ROW;
            case "ARROW_BATCH" -> BatchSerde.BATCH_SERDE_ARROW_BATCH;
            default -> throw new IllegalArgumentException(
                    "Unsupported shuffle serde: " + serde);
        };
    }

    private static ShufflePersistenceMode persistenceMode(String mode) {
        return switch (mode) {
            case "MEMORY_ONLY" -> ShufflePersistenceMode
                    .SHUFFLE_PERSISTENCE_MODE_MEMORY_ONLY;
            case "MEMORY_AND_DISK" -> ShufflePersistenceMode
                    .SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_DISK;
            case "MEMORY_AND_REMOTE" -> ShufflePersistenceMode
                    .SHUFFLE_PERSISTENCE_MODE_MEMORY_AND_REMOTE;
            default -> throw new IllegalArgumentException(
                    "Unsupported shuffle persistence mode: " + mode);
        };
    }

    private static Map<String, String> validatedSessionProperties(
            Map<String, String> sessionProperties) {
        Map<String, String> properties = sessionProperties == null
                ? Map.of() : sessionProperties;
        if (properties.containsKey(RESERVED_DEADLINE_SESSION_PROPERTY)) {
            throw new IllegalArgumentException(
                    RESERVED_DEADLINE_SESSION_PROPERTY
                            + " is a reserved task control field");
        }
        return properties;
    }
}
