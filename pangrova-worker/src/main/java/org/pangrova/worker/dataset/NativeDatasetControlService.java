package org.pangrova.worker.dataset;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.CanonicalTaskRequests;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.FinalizeDistributedWriteRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.jni.write.WorkerCatalogRuntime;
import org.pangrova.worker.task.TaskManager;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;

/** Authenticated Worker metadata control; never opens a page source or sink. */
@ThreadSafe
public final class NativeDatasetControlService implements AutoCloseable {
    public static final int MAX_REQUEST_BYTES = 16 * 1024 * 1024;
    private static final int MAX_FRAGMENTS = 4096;
    private static final int MAX_SCHEMA_NAMES = 4096;
    private final TaskManager tasks;
    private final WorkerCatalogRuntime catalogs;
    private final NativeDatasetControlAdmission admission = new NativeDatasetControlAdmission();

    /** Creates the synchronous control service with the production catalog authority. */
    public NativeDatasetControlService(TaskManager tasks, WorkerCatalogRuntime catalogs) {
        this.tasks = Objects.requireNonNull(tasks, "tasks");
        this.catalogs = Objects.requireNonNull(catalogs, "catalogs");
    }

    /** Acquires admission before buffering a bounded binary request body. */
    public byte[] execute(InputStream body) throws IOException {
        Objects.requireNonNull(body, "body");
        try (var slot = admission.acquire()) {
            byte[] bytes = body.readNBytes(MAX_REQUEST_BYTES + 1);
            if (bytes.length == 0 || bytes.length > MAX_REQUEST_BYTES) {
                throw new IllegalArgumentException("Native dataset control body exceeds metadata limits");
            }
            RuntimeCommand command = parse(bytes);
            Intent intent = validate(command);
            try (var catalog = catalogs.leaseControl(intent.catalog(), intent.security(), intent.output())) {
                requireDeadline(intent.deadline());
                if (intent.output().isPresent()) {
                    catalog.requireDatasetLocation(intent.output().get().getLance().getDatasetUri());
                }
                if (command.getCommandType() == org.pangrova.protocol.v2.RuntimeCommandType
                        .RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS) {
                    catalog.requireDatasetLocation(command.getPayload()
                            .unpack(org.pangrova.protocol.v2.DatasetScanPartitionsRequest.class).getDatasetUri());
                }
                byte[] result = NativeRuntime.nativeCommand(0, command.toBuilder()
                        .addWorkerLocalStorageBindings(catalog.storageBinding()).build().toByteArray());
                requireNativeResponse(result, command);
                return result;
            }
        }
    }

    private static void requireNativeResponse(byte[] bytes, RuntimeCommand command) {
        try {
            var result = RuntimeCommandResponse.parseFrom(bytes);
            if (!result.getSuccess()) {
                if (!result.hasError()) throw new IllegalStateException("Native command failed without error evidence");
                return;
            }
            String expected = switch (command.getCommandType()) {
                case RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT ->
                        org.pangrova.protocol.v2.DatasetSchemaFingerprintResult.getDescriptor().getFullName();
                case RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE ->
                        org.pangrova.protocol.v2.FinalizeDistributedWriteResult.getDescriptor().getFullName();
                case RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS ->
                        org.pangrova.protocol.v2.DatasetScanPartitionsResult.getDescriptor().getFullName();
                default -> throw new IllegalArgumentException("Not a dataset control operation");
            };
            if (!result.getPayload().getTypeUrl().equals("type.googleapis.com/" + expected)) {
                throw new IllegalStateException("Native metadata response payload type differs from command");
            }
        } catch (InvalidProtocolBufferException malformed) {
            throw new IllegalStateException("Malformed Native metadata response", malformed);
        }
    }

    private static RuntimeCommand parse(byte[] bytes) {
        try {
            return RuntimeCommand.parseFrom(bytes);
        } catch (InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Malformed canonical metadata command", malformed);
        }
    }

    private Intent validate(RuntimeCommand command) throws InvalidProtocolBufferException {
        if (!CanonicalTaskRequests.protocolVersion().equals(command.getProtocol()) || command.hasAttempt()) {
            throw new IllegalArgumentException("Metadata command must carry the canonical process protocol");
        }
        if (command.getWorkerLocalStorageBindingsCount() != 0) {
            throw new SecurityException("HTTP metadata control must not contain Worker-local storage settings");
        }
        return switch (command.getCommandType()) {
            case RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT -> validateSchema(command);
            case RUNTIME_COMMAND_TYPE_FINALIZE_DISTRIBUTED_WRITE -> validateFinalize(command);
            case RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS -> validateScan(command);
            default -> throw new IllegalArgumentException("Command is not a dataset metadata operation");
        };
    }

    private static Intent validateScan(RuntimeCommand command) throws InvalidProtocolBufferException {
        requirePayload(command, org.pangrova.protocol.v2.DatasetScanPartitionsRequest.getDescriptor().getFullName());
        var request = command.getPayload().unpack(org.pangrova.protocol.v2.DatasetScanPartitionsRequest.class);
        requireDeadline(request.getDeadlineEpochMs());
        requireCatalog(request.getCatalog());
        if (request.getVersion() <= 0 || request.getDatasetUri().isBlank()
                || request.getPageSize() < 1 || request.getPageSize() > 256
                || (request.getHasCursor() && request.getAfterPartitionId() < 0)) {
            throw new IllegalArgumentException("Dataset scan requires a bounded exact manifest partition page");
        }
        requireScanDigest(request.getPhysicalSchemaSha256());
        return new Intent(request.getCatalog(), request.getSecurity(), Optional.empty(), request.getDeadlineEpochMs());
    }

    private static void requireScanDigest(org.pangrova.protocol.v2.Checksum value) {
        if (value.getAlgorithm() != org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256
                || value.getDigest().size() != 32) {
            throw new IllegalArgumentException("Dataset scan requires an exact physical schema SHA-256");
        }
    }

    private static Intent validateSchema(RuntimeCommand command) throws InvalidProtocolBufferException {
        requirePayload(command, DatasetSchemaFingerprintRequest.getDescriptor().getFullName());
        var request = command.getPayload().unpack(DatasetSchemaFingerprintRequest.class);
        requireDeadline(request.getDeadlineEpochMs());
        if (!request.hasSchema() || request.getSchema().getNamesCount() == 0
                || request.getSchema().getNamesCount() > MAX_SCHEMA_NAMES
                || !request.getSchema().hasStruct()
                || request.getColumnTypesCount() != request.getSchema().getStruct().getTypesCount()) {
            throw new IllegalArgumentException("Schema fingerprint requires bounded ordered column types");
        }
        requireCatalog(request.getCatalog());
        return new Intent(request.getCatalog(), request.getSecurity(), Optional.empty(), request.getDeadlineEpochMs());
    }

    private Intent validateFinalize(RuntimeCommand command) throws InvalidProtocolBufferException {
        requirePayload(command, FinalizeDistributedWriteRequest.getDescriptor().getFullName());
        var request = command.getPayload().unpack(FinalizeDistributedWriteRequest.class);
        tasks.verifyControlEpochForAdmission(request.getControlEpoch());
        tasks.verifyExecutionOwnerForControl(request.getExecutionId(), request.getExecutionOwner());
        requireIntentOwner(request);
        requireDeadline(request.getDeadlineEpochMs());
        if (!request.getCommitFingerprint().matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Publication requires an exact commit fingerprint");
        }
        if (!request.getOutputBinding().hasLance()) {
            throw new IllegalArgumentException("Publication requires connector-owned Native allocation");
        }
        requireKnownPublication(request);
        validateFragments(request);
        CatalogExecutionBinding catalog = request.getOutputBinding().getLance().getCatalog();
        requireCatalog(catalog);
        return new Intent(catalog, request.getSecurity(), Optional.of(request.getOutputBinding()),
                request.getDeadlineEpochMs());
    }

    private static void requireIntentOwner(FinalizeDistributedWriteRequest request) {
        var intent = request.getIntentExecutionOwner();
        var authority = request.getExecutionOwner();
        if (!request.hasIntentExecutionOwner() || intent.getFencingToken() <= 0
                || intent.getOwnerId().isBlank() || intent.getOwnerIncarnation().isBlank()
                || authority.getFencingToken() < intent.getFencingToken()
                || (authority.getFencingToken() == intent.getFencingToken() && !authority.equals(intent))) {
            throw new IllegalArgumentException("Publication intent owner differs from immutable winner provenance");
        }
    }

    private static void requireKnownPublication(FinalizeDistributedWriteRequest request) {
        var checksum = request.getKnownManifestSha256();
        if (request.getKnownPublishedVersion() == 0) {
            if (request.hasKnownManifestSha256()) {
                throw new IllegalArgumentException("Manifest digest requires a known publication version");
            }
            return;
        }
        if (!request.getRecovering() || request.getKnownPublishedVersion() < 0
                || checksum.getAlgorithm() != org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256
                || checksum.getDigest().size() != 32) {
            throw new IllegalArgumentException("Known publication requires exact version, SHA-256 and recovery intent");
        }
    }

    private static void validateFragments(FinalizeDistributedWriteRequest request) {
        if (request.getFragmentsCount() == 0 || request.getFragmentsCount() > MAX_FRAGMENTS) {
            throw new IllegalArgumentException("Publication requires bounded nonempty winner metadata");
        }
        var workUnits = new HashSet<String>();
        for (var fragment : request.getFragmentsList()) {
            var attempt = fragment.getTaskAttempt();
            requireWinnerProvenance(request, attempt);
            if (fragment.getCommitMetadata().isEmpty()
                    || !CanonicalTaskRequests.sha256(fragment.getCommitMetadata().toByteArray())
                            .equals(fragment.getSha256())
                    || !workUnits.add(attempt.getStageId() + ":" + attempt.getPartitionId())) {
                throw new IllegalArgumentException("Winner metadata identity/checksum differs from publication");
            }
            // Validates the complete owner/control/plan/attempt fence; recovery may
            // intentionally carry winners from an earlier execution owner.
            org.pangrova.protocol.NativeWriteBindings.attemptPrefix("control-validation", attempt);
        }
    }

    private static void requireWinnerProvenance(FinalizeDistributedWriteRequest request,
            org.pangrova.protocol.v2.TaskAttemptId attempt) {
        if (!request.getExecutionId().equals(attempt.getQueryOrJobId())
                || !attempt.getExecutionOwner().equals(request.getIntentExecutionOwner())
                || attempt.getControlEpoch() <= 0 || attempt.getControlEpoch() > request.getControlEpoch()) {
            throw new IllegalArgumentException("Winner provenance differs from original publication intent");
        }
    }

    private static void requireCatalog(CatalogExecutionBinding catalog) {
        if (catalog.getCatalogName().isBlank() || catalog.getConnectorProvider().isBlank()
                || catalog.getCatalogIncarnationId().isBlank() || catalog.getCatalogRevision() <= 0
                || catalog.getConfigurationFingerprint().getAlgorithm()
                        != org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256
                || catalog.getConfigurationFingerprint().getDigest().size() != 32) {
            throw new IllegalArgumentException("Native dataset control requires exact catalog deployment");
        }
    }

    private static void requirePayload(RuntimeCommand command, String name) {
        if (!command.getPayload().getTypeUrl().equals("type.googleapis.com/" + name)) {
            throw new IllegalArgumentException("Native dataset control payload type is not canonical");
        }
    }

    private static void requireDeadline(long deadline) {
        if (deadline <= 0 || deadline <= System.currentTimeMillis()) throw new ControlDeadlineExceededException();
    }

    @Override
    public void close() { admission.close(); }

    private record Intent(CatalogExecutionBinding catalog, SecurityContext security,
                          Optional<WriteOutputBinding> output, long deadline) {}
}
