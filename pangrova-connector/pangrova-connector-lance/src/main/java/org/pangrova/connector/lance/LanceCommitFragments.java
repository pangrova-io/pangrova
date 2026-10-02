package org.pangrova.connector.lance;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.CatalogDeploymentFingerprint;
import org.pangrova.protocol.NativeWriteBindings;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.LanceWriteFragment;
import org.pangrova.protocol.v2.LanceTransactionReference;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.HexFormat;

/** Validates bounded winner metadata against one immutable connector write intent. */
final class LanceCommitFragments {
    private static final int MAX_FRAGMENTS = 4096;
    private static final int MAX_FRAGMENT_BYTES = 64 * 1024;

    private LanceCommitFragments() {}

    static long validate(LanceOutputHandle output, List<byte[]> fragments, DistributedWriteCommitContext context) {
        if (fragments.isEmpty() || fragments.size() > MAX_FRAGMENTS) {
            throw new IllegalArgumentException("Lance write requires bounded nonempty winner fragments");
        }
        validateHandle(output, context);
        Set<String> workUnits = new HashSet<>();
        long rows = 0;
        for (byte[] bytes : fragments) {
            if (bytes == null || bytes.length == 0 || bytes.length > MAX_FRAGMENT_BYTES) {
                throw new IllegalArgumentException("Lance fragment exceeds metadata size limit");
            }
            LanceWriteFragment fragment = parse(bytes);
            validateFragment(output, fragment, context);
            var attempt = fragment.getTaskAttempt();
            if (!workUnits.add(attempt.getStageId() + ":" + attempt.getPartitionId())) {
                throw new IllegalArgumentException("Lance winner fragments repeat a logical work unit");
            }
            rows = Math.addExact(rows, fragment.getRowsWritten());
        }
        return rows;
    }

    private static void validateHandle(LanceOutputHandle output, DistributedWriteCommitContext context) {
        try {
            var binding = WriteOutputBinding.parseFrom(context.writeOutputBinding());
            var handle = binding.getLance().getOutputHandle();
            var codec = new LanceOutputHandleCodec();
            byte[] bytes = codec.serialize(output);
            if (!binding.hasLance()
                    || !binding.getLance().getCatalog().getCatalogName().equals(output.table().catalog())) {
                throw new IllegalArgumentException("Lance output handle differs from durable intent");
            }
            validateEnvelope(codec, bytes, handle);
        } catch (InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Malformed Lance durable write binding", malformed);
        }
    }

    private static void validateEnvelope(LanceOutputHandleCodec codec, byte[] bytes,
            SerializedConnectorOutputHandle handle) {
        if (!codec.typeUrl().equals(handle.getTypeUrl()) || codec.codecVersion() != handle.getCodecVersion()
                || !MessageDigest.isEqual(bytes, handle.getPayload().toByteArray())
                || !CatalogDeploymentFingerprint.sha256(bytes).equals(handle.getPayloadChecksum())) {
            throw new IllegalArgumentException("Lance output handle differs from durable intent");
        }
    }

    private static LanceWriteFragment parse(byte[] bytes) {
        try {
            return LanceWriteFragment.parseFrom(bytes);
        } catch (InvalidProtocolBufferException malformed) {
            throw new IllegalArgumentException("Malformed Lance fragment", malformed);
        }
    }

    private static void validateFragment(LanceOutputHandle output, LanceWriteFragment fragment,
            DistributedWriteCommitContext context) {
        var allocation = output.allocation();
        validateAllocation(allocation, fragment);
        validateReferenceIdentity(allocation, fragment, context);
        validateReferenceLocation(allocation, fragment);
    }

    private static void validateAllocation(NativeWriteAllocation allocation, LanceWriteFragment fragment) {
        String operation = fragment.getOperation().name().replace("LANCE_WRITE_OPERATION_", "");
        if (fragment.getContractVersion() != 2 || !allocation.datasetUri().equals(fragment.getDatasetUri())
                || !allocation.writeId().equals(fragment.getWriteId())
                || !allocation.operation().name().equals(operation)
                || allocation.readVersion() != fragment.getReadVersion()
                || !MessageDigest.isEqual(allocation.schemaFenceSha256(),
                        fragment.getSchemaFenceSha256().toByteArray())) {
            throw new IllegalArgumentException("Lance fragment differs from fenced winner allocation");
        }
    }

    private static void validateReferenceIdentity(NativeWriteAllocation allocation, LanceWriteFragment fragment,
            DistributedWriteCommitContext context) {
        LanceTransactionReference reference = fragment.getTransaction();
        if (!context.executionId().equals(fragment.getTaskAttempt().getQueryOrJobId())
                || !reference.getTaskAttempt().equals(fragment.getTaskAttempt())
                || !reference.getTenantId().equals(context.tenantId())
                || !reference.getWriteId().equals(allocation.writeId())
                || !reference.getGeneration().equals(allocation.generation())) {
            throw new IllegalArgumentException("Lance fragment reference identity differs from durable intent");
        }
    }

    private static void validateReferenceLocation(NativeWriteAllocation allocation, LanceWriteFragment fragment) {
        LanceTransactionReference reference = fragment.getTransaction();
        String prefix = NativeWriteBindings.attemptPrefix(
                allocation.stagingPrefix(), fragment.getTaskAttempt());
        String expected = prefix + "/transaction-"
                + HexFormat.of().formatHex(reference.getSha256().getDigest().toByteArray()) + ".pb";
        if (reference.getSizeBytes() <= 0 || reference.getSizeBytes() > 64 * 1024 * 1024
                || reference.getSha256().getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || reference.getSha256().getDigest().size() != 32
                || !reference.getRelativeObjectKey().equals(expected)) {
            throw new IllegalArgumentException("Lance fragment reference location or bounds are invalid");
        }
        if (fragment.getRowsWritten() < 0 || fragment.getInputBytes() < 0) {
            throw new IllegalArgumentException("Lance fragment usage exceeds signed control-plane bounds");
        }
    }
}
