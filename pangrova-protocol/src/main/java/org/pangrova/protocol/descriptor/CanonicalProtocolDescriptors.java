package org.pangrova.protocol.descriptor;

import org.pangrova.common.util.DigestUtils;

import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Deterministic descriptor sets for the canonical JVM/native boundaries. */
public final class CanonicalProtocolDescriptors {

    private CanonicalProtocolDescriptors() {}

    /** Returns the sole canonical descriptor set rooted at v2/runtime.proto. */
    public static FileDescriptorSet descriptorSet() {
        return descriptorSet(org.pangrova.protocol.v2.Runtime.getDescriptor(), true);
    }

    private static FileDescriptorSet descriptorSet(
            FileDescriptor root,
            boolean normalizeDerivedFields) {
        List<FileDescriptor> descriptors = new ArrayList<>();
        collect(root, new HashSet<>(), descriptors);
        descriptors.sort(Comparator.comparing(FileDescriptor::getName));
        FileDescriptorSet.Builder result = FileDescriptorSet.newBuilder();
        descriptors.forEach(value -> result.addFile(normalizeDerivedFields
                ? normalize(value.toProto())
                : value.toProto()));
        return result.build();
    }

    /**
     * Removes generator-derived JSON names before cross-runtime hashing.
     *
     * <p>JSON names are derived from protobuf field names and do not change the
     * binary wire contract. Normalizing them keeps JVM/C++ descriptor hashing
     * independent of generator implementation details.</p>
     */
    private static FileDescriptorProto normalize(FileDescriptorProto descriptor) {
        FileDescriptorProto.Builder normalized = descriptor.toBuilder();
        normalized.getMessageTypeBuilderList().forEach(
                CanonicalProtocolDescriptors::clearDerivedFields);
        normalized.getExtensionBuilderList().forEach(
                FieldDescriptorProto.Builder::clearJsonName);
        return normalized.build();
    }

    private static void clearDerivedFields(DescriptorProto.Builder message) {
        message.getFieldBuilderList().forEach(FieldDescriptorProto.Builder::clearJsonName);
        message.getExtensionBuilderList().forEach(FieldDescriptorProto.Builder::clearJsonName);
        message.getNestedTypeBuilderList().forEach(
                CanonicalProtocolDescriptors::clearDerivedFields);
    }

    /** Returns the stable wire representation used by the native handshake. */
    public static byte[] descriptorSetBytes() {
        return descriptorSet().toByteArray();
    }

    /** Returns SHA-256 of {@link #descriptorSetBytes()}. */
    public static byte[] sha256() {
        return sha256(descriptorSetBytes());
    }

    private static byte[] sha256(byte[] descriptorBytes) {
        return DigestUtils.sha256().digest(descriptorBytes);
    }

    private static void collect(
            FileDescriptor descriptor,
            Set<String> visited,
            List<FileDescriptor> output) {
        if (!visited.add(descriptor.getName())) {
            return;
        }
        descriptor.getDependencies().forEach(value -> collect(value, visited, output));
        output.add(descriptor);
    }
}
