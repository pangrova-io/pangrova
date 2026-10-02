package org.pangrova.protocol;

import com.google.protobuf.CodedOutputStream;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.PhysicalFileWriteBinding;

import java.io.IOException;
import java.util.HexFormat;

/** Binds Native output staging to the complete canonical task attempt. */
public final class NativeWriteBindings {
    private NativeWriteBindings() {}

    /** Returns the immutable request with its attempt-private Native staging path. */
    public static TaskRequest bindAttempt(TaskRequest request) {
        var output = request.getExecutionOptions().getWrite().getOutputBinding();
        if (output.hasPhysical()) {
            return request.toBuilder().setExecutionOptions(request.getExecutionOptions().toBuilder()
                    .setWrite(request.getExecutionOptions().getWrite().toBuilder()
                            .setOutputBinding(output.toBuilder().setPhysical(
                                    bindPhysical(output.getPhysical(), request.getAttempt()))))).build();
        }
        if (!output.hasLance()) return request;
        var lance = output.getLance();
        String prefix = attemptPrefix(lance.getAllocationStagingPrefix(), request.getAttempt());
        return request.toBuilder().setExecutionOptions(request.getExecutionOptions().toBuilder()
                .setWrite(request.getExecutionOptions().getWrite().toBuilder()
                        .setOutputBinding(request.getExecutionOptions().getWrite().getOutputBinding().toBuilder()
                                .setLance(lance.toBuilder().setTransactionStagingPrefix(prefix))))).build();
    }

    /** Binds exact SDK file names to one complete fenced attempt without changing bucket or source snapshot. */
    public static PhysicalFileWriteBinding bindPhysical(PhysicalFileWriteBinding template, TaskAttemptId attempt) {
        if (!template.getAttemptPrefix().isEmpty() || template.getFilesList().stream().anyMatch(file -> file.hasTaskAttempt())) {
            throw new IllegalArgumentException("Physical allocation template has already been bound to an attempt");
        }
        String prefix = attemptPrefix(template.getAllocationPrefix(), attempt);
        String suffix = prefix.substring(prefix.lastIndexOf('/') + 1);
        var bound = template.toBuilder().setAttemptPrefix(prefix).clearFiles();
        for (var file : template.getFilesList()) {
            String name = file.getFileName();
            String location = file.getOpaqueLocation();
            if (!name.endsWith(".lance") || !location.endsWith("/" + name)
                    || !location.substring(0, location.length() - name.length() - 1).equals(template.getAllocationPrefix())) {
                throw new IllegalArgumentException("Physical file escapes its exact SDK allocation directory");
            }
            String attemptName = name.substring(0, name.length() - ".lance".length()) + "." + suffix + ".lance";
            bound.addFiles(PhysicalFileWriteBindings.sealed(file.toBuilder().setTaskAttempt(attempt)
                    .setFileName(attemptName).setOpaqueLocation(template.getAllocationPrefix() + "/" + attemptName)));
        }
        return bound.build();
    }

    /** Derives the identical path used by Java admission and Native parsing. */
    public static String attemptPrefix(String allocationPrefix, TaskAttemptId attempt) {
        if (allocationPrefix == null || allocationPrefix.isBlank()) {
            throw new IllegalArgumentException("Native output requires a fenced attempt and allocation prefix");
        }
        requireFencedAttempt(attempt);
        byte[] bytes = new byte[attempt.getSerializedSize()];
        CodedOutputStream output = CodedOutputStream.newInstance(bytes);
        output.useDeterministicSerialization();
        try {
            attempt.writeTo(output);
            output.checkNoSpaceLeft();
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot serialize Native output attempt", failure);
        }
        return allocationPrefix + "/attempt-" + HexFormat.of().formatHex(
                CanonicalTaskRequests.sha256(bytes).getDigest().toByteArray());
    }

    private static void requireFencedAttempt(TaskAttemptId attempt) {
        if (!attempt.hasExecutionOwner() || attempt.getControlEpoch() == 0
                || attempt.getFencingToken() == 0 || attempt.getPlanEpoch() == 0
                || attempt.getTaskId().isBlank()) {
            throw new IllegalArgumentException("Native output requires a fenced attempt and allocation prefix");
        }
    }
}
