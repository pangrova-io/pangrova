package org.pangrova.worker.jni.write;

import org.pangrova.protocol.NativeWriteBindings;
import org.pangrova.protocol.PhysicalFileWriteBindings;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.FileFormat;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.PhysicalFileWriteBinding;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import java.security.MessageDigest;

/** Compares canonical Native bindings to the connector's exact decoded allocation. */
final class NativeOutputAllocationValidation {
    private NativeOutputAllocationValidation() {}

    static void validateTask(TaskRequest request, ConnectorOutputTableHandle handle) {
        var output = request.getExecutionOptions().getWrite().getOutputBinding();
        if (output.hasPhysical()) {
            var physical = output.getPhysical();
            validate(physical, handle);
            if (request.getExecutionOptions().getWrite().getFileFormat() != physical.getFileFormat()
                    || physical.getFilesList().stream().anyMatch(file -> !file.getTaskAttempt().equals(request.getAttempt()))
                    || !NativeWriteBindings.attemptPrefix(physical.getAllocationPrefix(), request.getAttempt())
                            .equals(physical.getAttemptPrefix())) throw mismatch();
            return;
        }
        var binding = output.getLance();
        validate(binding, handle);
        var allocation = handle.nativeWriteAllocation().orElseThrow();
        if (request.getExecutionOptions().getWrite().getFileFormat() != FileFormat.FILE_FORMAT_LANCE
                || !NativeWriteBindings.attemptPrefix(allocation.stagingPrefix(), request.getAttempt())
                        .equals(binding.getTransactionStagingPrefix())) {
            throw mismatch();
        }
    }

    static void validate(PhysicalFileWriteBinding binding, ConnectorOutputTableHandle handle) {
        var allocation = handle.nativePhysicalWriteAllocation().orElseThrow(() ->
                new IllegalArgumentException("Physical binding requires the exact connector physical output handle"));
        var expected = PhysicalFileWriteBindings.template(allocation).toBuilder()
                .setCatalog(binding.getCatalog()).setOutputHandle(binding.getOutputHandle()).build();
        if (!binding.getAttemptPrefix().isEmpty()) {
            if (binding.getFilesCount() == 0 || !binding.getFiles(0).hasTaskAttempt()) throw mismatch();
            expected = NativeWriteBindings.bindPhysical(expected, binding.getFiles(0).getTaskAttempt());
        }
        if (!expected.equals(binding)) throw mismatch();
    }

    static void validate(LanceWriteBinding binding, ConnectorOutputTableHandle handle) {
        var allocation = handle.nativeWriteAllocation().orElseThrow(() ->
                new IllegalArgumentException("Native binding requires a Native connector output handle"));
        String operation = binding.getOperation().name().replace("LANCE_WRITE_OPERATION_", "");
        if (!allocation.datasetUri().equals(binding.getDatasetUri())
                || !allocation.operation().name().equals(operation)
                || allocation.readVersion() != binding.getExpectedReadVersion()
                || !allocation.writeId().equals(binding.getWriteId())
                || !allocation.generation().equals(binding.getGeneration())) {
            throw mismatch();
        }
        validateSchemaAndStaging(binding, allocation);
    }

    private static void validateSchemaAndStaging(LanceWriteBinding binding, NativeWriteAllocation allocation) {
        if (!allocation.stagingPrefix().equals(binding.getAllocationStagingPrefix())
                || !allocation.writeColumnNames().equals(binding.getWriteColumnNamesList())
                || binding.getSchemaFenceSha256().getAlgorithm() != Checksum.Algorithm.ALGORITHM_SHA256
                || !MessageDigest.isEqual(allocation.schemaFenceSha256(),
                        binding.getSchemaFenceSha256().getDigest().toByteArray())) {
            throw mismatch();
        }
    }

    private static IllegalArgumentException mismatch() {
        return new IllegalArgumentException("Native output binding differs from connector allocation");
    }
}
