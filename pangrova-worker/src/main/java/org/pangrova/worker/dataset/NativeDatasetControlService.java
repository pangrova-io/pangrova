package org.pangrova.worker.dataset;

import org.pangrova.common.crypto.Sha256Digests;

import org.pangrova.worker.exception.ControlDeadlineExceededException;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.worker.jni.NativeRuntime;
import org.pangrova.worker.catalog.WorkerCatalogRuntime;
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
    private static final int MAX_OBJECT_PAGES = 4096;
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
                throw new IllegalArgumentException(
                        "Native dataset control body exceeds metadata limits");
            }
            RuntimeCommand command = parse(bytes);
            Intent intent = validate(command);
            try (var catalog =
                    catalogs.leaseControl(intent.catalog(), intent.security(), intent.output())) {
                requireDeadline(intent.deadline());
                if (intent.output().isPresent()) {
                    catalog.requireDatasetLocation(
                            intent.output().get().getLance().getDatasetUri());
                }
                if (command.getCommandType()
                        == org.pangrova.protocol.v2.RuntimeCommandType
                                .RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS) {
                    catalog.requireDatasetLocation(
                            command.getPayload()
                                    .unpack(
                                            org.pangrova.protocol.v2.DatasetScanPartitionsRequest
                                                    .class)
                                    .getDatasetUri());
                }
                if (command.getCommandType()
                        == org.pangrova.protocol.v2.RuntimeCommandType
                                .RUNTIME_COMMAND_TYPE_CLEANUP_SOURCE_OBJECTS) {
                    catalog.requireDatasetLocation(
                            command.getPayload()
                                    .unpack(
                                            org.pangrova.protocol.v2.CleanupSourceObjectsRequest
                                                    .class)
                                    .getDatasetUri());
                }
                byte[] result =
                        NativeRuntime.nativeCommand(
                                0,
                                command.toBuilder()
                                        .addWorkerLocalStorageBindings(catalog.storageBinding())
                                        .build()
                                        .toByteArray());
                requireNativeResponse(result, command);
                return result;
            }
        }
    }

    private static void requireNativeResponse(byte[] bytes, RuntimeCommand command) {
        if (bytes.length == 0 || bytes.length > MAX_REQUEST_BYTES) {
            throw new IllegalStateException(
                    "Native metadata response exceeds its canonical envelope bound");
        }
        try {
            var result = RuntimeCommandResponse.parseFrom(bytes);
            if (!result.getSuccess()) {
                if (!result.hasError()) {
                    throw new IllegalStateException("Native command failed without error evidence");
                }
                return;
            }
            String expected = switch (command.getCommandType()) {
                case RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT ->
                                org.pangrova.protocol.v2.DatasetSchemaFingerprintResult
                                        .getDescriptor()
                                        .getFullName();
                case RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE ->
                                org.pangrova.protocol.v2.PrepareDistributedWriteResult
                                        .getDescriptor()
                                        .getFullName();
                case RUNTIME_COMMAND_TYPE_CLEANUP_SOURCE_OBJECTS ->
                                org.pangrova.protocol.v2.CleanupSourceObjectsResult.getDescriptor()
                                        .getFullName();
                case RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS ->
                                org.pangrova.protocol.v2.DatasetScanPartitionsResult.getDescriptor()
                                        .getFullName();
                default ->
                                throw new IllegalArgumentException(
                                        "Not a dataset control operation");
            };
            if (!result.getPayload().getTypeUrl().equals("type.googleapis.com/" + expected)) {
                throw new IllegalStateException(
                        "Native metadata response payload type differs from command");
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
        if (!CanonicalProtocolIdentity.current().equals(command.getProtocol())
                || command.hasAttempt()) {
            throw new IllegalArgumentException(
                    "Metadata command must carry the canonical process protocol");
        }
        if (command.getWorkerLocalStorageBindingsCount() != 0) {
            throw new SecurityException(
                    "HTTP metadata control must not contain Worker-local storage settings");
        }
        return switch (command.getCommandType()) {
            case RUNTIME_COMMAND_TYPE_DATASET_SCHEMA_FINGERPRINT -> validateSchema(command);
            case RUNTIME_COMMAND_TYPE_PREPARE_DISTRIBUTED_WRITE -> validatePrepare(command);
            case RUNTIME_COMMAND_TYPE_CLEANUP_SOURCE_OBJECTS -> validateCleanup(command);
            case RUNTIME_COMMAND_TYPE_DATASET_SCAN_PARTITIONS -> validateScan(command);
            default ->
                    throw new IllegalArgumentException(
                            "Command is not a dataset metadata operation");
        };
    }

    private Intent validateCleanup(RuntimeCommand command) throws InvalidProtocolBufferException {
        requirePayload(
                command,
                org.pangrova.protocol.v2.CleanupSourceObjectsRequest.getDescriptor().getFullName());
        var request =
                command.getPayload()
                        .unpack(org.pangrova.protocol.v2.CleanupSourceObjectsRequest.class);
        tasks.verifyControlEpochForAdmission(request.getControlEpoch());
        tasks.verifyExecutionOwnerForControl(request.getExecutionId(), request.getExecutionOwner());
        requireDeadline(request.getDeadlineEpochMs());
        requireCatalog(request.getCatalog());
        if (request.getExecutionId().isBlank()
                || !Sha256Digests.isSha256Hex(request.getCommitFingerprint())
                || !request.getCleanupId().matches("[0-9a-f]{32}")
                || request.getDatasetUri().isBlank()
                || request.getObjectsCount() < 1
                || request.getObjectsCount() > 512) {
            throw new IllegalArgumentException(
                    "Source cleanup requires an exact bounded authoritative batch");
        }
        requireScanDigest(request.getObjectsSha256());
        return new Intent(
                request.getCatalog(),
                request.getSecurity(),
                Optional.empty(),
                request.getDeadlineEpochMs());
    }

    private static Intent validateScan(RuntimeCommand command)
            throws InvalidProtocolBufferException {
        requirePayload(
                command,
                org.pangrova.protocol.v2.DatasetScanPartitionsRequest.getDescriptor()
                        .getFullName());
        var request =
                command.getPayload()
                        .unpack(org.pangrova.protocol.v2.DatasetScanPartitionsRequest.class);
        requireDeadline(request.getDeadlineEpochMs());
        requireCatalog(request.getCatalog());
        if (request.getVersion() <= 0
                || request.getDatasetUri().isBlank()
                || request.getPageSize() < 1
                || request.getPageSize() > 256
                || (request.getHasCursor() && request.getAfterPartitionId() < 0)) {
            throw new IllegalArgumentException(
                    "Dataset scan requires a bounded exact manifest partition page");
        }
        requireScanDigest(request.getPhysicalSchemaSha256());
        if (!request.hasAcceptedSnapshot()) {
            throw new IllegalArgumentException("Dataset scan requires accepted membership");
        }
        var reference =
                org.pangrova.protocol.source.SourceSnapshotCodec.decode(
                        request.getAcceptedSnapshot(), request.getCatalog());
        if (!reference.tenant().equals(request.getSecurity().getTenantId())
                || reference.version() != request.getVersion()
                || !reference.datasetUri().equals(request.getDatasetUri())
                || !request.getAcceptedSnapshot()
                        .getPhysicalSchemaSha256()
                        .equals(request.getPhysicalSchemaSha256())) {
            throw new IllegalArgumentException("Dataset scan differs from its accepted identity");
        }
        return new Intent(
                request.getCatalog(),
                request.getSecurity(),
                Optional.empty(),
                request.getDeadlineEpochMs());
    }

    private static void requireScanDigest(org.pangrova.protocol.v2.Checksum value) {
        if (value.getAlgorithm() != org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256
                || value.getDigest().size() != 32) {
            throw new IllegalArgumentException(
                    "Dataset scan requires an exact physical schema SHA-256");
        }
    }

    private static Intent validateSchema(RuntimeCommand command)
            throws InvalidProtocolBufferException {
        requirePayload(command, DatasetSchemaFingerprintRequest.getDescriptor().getFullName());
        var request = command.getPayload().unpack(DatasetSchemaFingerprintRequest.class);
        requireDeadline(request.getDeadlineEpochMs());
        if (!request.hasSchema()
                || request.getSchema().getNamesCount() == 0
                || request.getSchema().getNamesCount() > MAX_SCHEMA_NAMES
                || !request.getSchema().hasStruct()
                || request.getColumnTypesCount()
                        != request.getSchema().getStruct().getTypesCount()) {
            throw new IllegalArgumentException(
                    "Schema fingerprint requires bounded ordered column types");
        }
        requireCatalog(request.getCatalog());
        return new Intent(
                request.getCatalog(),
                request.getSecurity(),
                Optional.empty(),
                request.getDeadlineEpochMs());
    }

    private Intent validatePrepare(RuntimeCommand command) throws InvalidProtocolBufferException {
        requirePayload(command, PrepareDistributedWriteRequest.getDescriptor().getFullName());
        var request = command.getPayload().unpack(PrepareDistributedWriteRequest.class);
        tasks.verifyControlEpochForAdmission(request.getControlEpoch());
        tasks.verifyExecutionOwnerForControl(request.getExecutionId(), request.getExecutionOwner());
        requireIntentOwner(request);
        requireDeadline(request.getDeadlineEpochMs());
        if (!request.getInvocationId().matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException("Preparation requires its durable invocation nonce");
        }
        if (!Sha256Digests.isSha256Hex(request.getCommitFingerprint())) {
            throw new IllegalArgumentException("Publication requires an exact commit fingerprint");
        }
        if (!request.getOutputBinding().hasLance()) {
            throw new IllegalArgumentException(
                    "Publication requires connector-owned Native allocation");
        }
        requireReservation(request);
        validateFragments(request);
        CatalogExecutionBinding catalog = request.getOutputBinding().getLance().getCatalog();
        requireCatalog(catalog);
        return new Intent(
                catalog,
                request.getSecurity(),
                Optional.of(request.getOutputBinding()),
                request.getDeadlineEpochMs());
    }

    private static void requireIntentOwner(PrepareDistributedWriteRequest request) {
        var intent = request.getIntentExecutionOwner();
        var authority = request.getExecutionOwner();
        if (!request.hasIntentExecutionOwner()
                || intent.getFencingToken() <= 0
                || intent.getOwnerId().isBlank()
                || intent.getOwnerIncarnation().isBlank()
                || authority.getFencingToken() < intent.getFencingToken()
                || (authority.getFencingToken() == intent.getFencingToken()
                        && !authority.equals(intent))) {
            throw new IllegalArgumentException(
                    "Publication intent owner differs from immutable winner provenance");
        }
    }

    private static void requireReservation(PrepareDistributedWriteRequest request) {
        var reservation = request.getReservation();
        var binding = request.getOutputBinding().getLance();
        requireReservedIdentityAndCursor(request);
        if (binding.hasBaseSnapshot()) {
            var base =
                    org.pangrova.protocol.source.SourceSnapshotCodec.decode(
                            binding.getBaseSnapshot(), binding.getCatalog());
            if (!base.tenant().equals(reservation.getTenantId())
                    || !base.tableId().equals(reservation.getTableId())
                    || base.generation() != reservation.getTableGeneration()
                    || base.version() != binding.getExpectedReadVersion()
                    || !base.datasetUri().equals(binding.getDatasetUri())
                    || !binding.getBaseSnapshot()
                            .getPhysicalSchemaSha256()
                            .equals(binding.getSchemaFenceSha256())) {
                throw new IllegalArgumentException(
                        "Preparation accepted base differs from reserved table");
            }
        }
    }

    private static void requireReservedIdentityAndCursor(PrepareDistributedWriteRequest request) {
        var reservation = request.getReservation();
        var binding = request.getOutputBinding().getLance();
        if (!request.hasReservation()
                || reservation.getTableId().isBlank()
                || reservation.getTableGeneration() <= 0
                || !reservation.getTenantId().equals(request.getSecurity().getTenantId())
                || !reservation
                        .getManifestRelativePath()
                        .equals(
                                "_pangrova/prepared/"
                                        + request.getCommitFingerprint()
                                        + ".manifest")
                || reservation.hasBaseSnapshot() != binding.hasBaseSnapshot()
                || (binding.getExpectedReadVersion() == 0) != !binding.hasBaseSnapshot()
                || (binding.hasBaseSnapshot()
                        && !reservation.getBaseSnapshot().equals(binding.getBaseSnapshot()))
                || !validObjectPageCursor(request)) {
            throw new IllegalArgumentException(
                    "Preparation differs from reserved identity, accepted base or page cursor");
        }
    }

    private static boolean validObjectPageCursor(PrepareDistributedWriteRequest request) {
        return request.getObjectPageSize() == 512
                && request.getObjectPageIndex() >= 0
                && request.getObjectPageIndex() < MAX_OBJECT_PAGES
                && ((request.getObjectPageIndex() == 0) == request.getAfterObjectPath().isEmpty());
    }

    private static void validateFragments(PrepareDistributedWriteRequest request) {
        if (request.getFragmentsCount() == 0 || request.getFragmentsCount() > MAX_FRAGMENTS) {
            throw new IllegalArgumentException(
                    "Publication requires bounded nonempty winner metadata");
        }
        var workUnits = new HashSet<String>();
        for (var fragment : request.getFragmentsList()) {
            var attempt = fragment.getTaskAttempt();
            requireWinnerProvenance(request, attempt);
            if (fragment.getCommitMetadata().isEmpty()
                    || !ProtocolChecksums.sha256(fragment.getCommitMetadata().toByteArray())
                            .equals(fragment.getSha256())
                    || !workUnits.add(attempt.getStageId() + ":" + attempt.getPartitionId())) {
                throw new IllegalArgumentException(
                        "Winner metadata identity/checksum differs from publication");
            }
            // Validates the complete owner/control/plan/attempt fence; recovery may
            // intentionally carry winners from an earlier execution owner.
            org.pangrova.protocol.write.NativeWriteBinder.attemptPrefix("control-validation", attempt);
        }
    }

    private static void requireWinnerProvenance(
            PrepareDistributedWriteRequest request,
            org.pangrova.protocol.v2.TaskAttemptId attempt) {
        if (!request.getExecutionId().equals(attempt.getQueryOrJobId())
                || !attempt.getExecutionOwner().equals(request.getIntentExecutionOwner())
                || attempt.getControlEpoch() <= 0
                || attempt.getControlEpoch() > request.getControlEpoch()) {
            throw new IllegalArgumentException(
                    "Winner provenance differs from original publication intent");
        }
    }

    private static void requireCatalog(CatalogExecutionBinding catalog) {
        if (catalog.getCatalogName().isBlank()
                || catalog.getConnectorProvider().isBlank()
                || catalog.getCatalogIncarnationId().isBlank()
                || catalog.getCatalogRevision() <= 0
                || catalog.getConfigurationFingerprint().getAlgorithm()
                        != org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256
                || catalog.getConfigurationFingerprint().getDigest().size() != 32) {
            throw new IllegalArgumentException(
                    "Native dataset control requires exact catalog deployment");
        }
    }

    private static void requirePayload(RuntimeCommand command, String name) {
        if (!command.getPayload().getTypeUrl().equals("type.googleapis.com/" + name)) {
            throw new IllegalArgumentException(
                    "Native dataset control payload type is not canonical");
        }
    }

    private static void requireDeadline(long deadline) {
        if (deadline <= 0 || deadline <= System.currentTimeMillis()) {
            throw new ControlDeadlineExceededException();
        }
    }

    @Override
    public void close() {
        admission.close();
    }

    private record Intent(
            CatalogExecutionBinding catalog,
            SecurityContext security,
            Optional<WriteOutputBinding> output,
            long deadline) {}
}
