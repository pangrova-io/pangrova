package org.pangrova.generation.api.dependency;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

/** Canonically ordered dependency set and its stable SHA-256 identity. */
public record GenerationDependencySet(List<GenerationDependency> dependencies, String sha256) {

    private static final Comparator<String> UTF8_ORDER =
            GenerationDependencySet::compareUnsignedUtf8;
    private static final Comparator<GenerationDependency> ORDER = Comparator
            .comparing(GenerationDependency::kind, UTF8_ORDER)
            .thenComparing(GenerationDependency::id, UTF8_ORDER)
            .thenComparing(GenerationDependency::version, UTF8_ORDER);

    public GenerationDependencySet {
        dependencies = List.copyOf(dependencies).stream().sorted(ORDER).toList();
        if (dependencies.isEmpty()) {
            throw new IllegalArgumentException("artifact dependency set must not be empty");
        }
        Set<LogicalIdentity> identities = new HashSet<>();
        for (GenerationDependency dependency : dependencies) {
            if (!identities.add(new LogicalIdentity(
                    dependency.kind(), dependency.id()))) {
                throw new IllegalArgumentException(
                        "artifact dependency set contains a duplicate logical identity");
            }
        }
        String actual = digest(dependencies);
        if (sha256 == null || !sha256.matches("[0-9a-fA-F]{64}")
                || !actual.equalsIgnoreCase(sha256)) {
            throw new IllegalArgumentException(
                    "artifact dependency set checksum differs from canonical contents");
        }
        sha256 = actual;
    }

    public static GenerationDependencySet of(List<GenerationDependency> dependencies) {
        List<GenerationDependency> normalized = List.copyOf(dependencies).stream()
                .sorted(ORDER).toList();
        return new GenerationDependencySet(normalized, digest(normalized));
    }

    /** Canonical singleton form used by existing table-snapshot artifact jobs. */
    public static GenerationDependencySet sourceSnapshot(String sourceId, String snapshot) {
        return of(List.of(new GenerationDependency("SOURCE_SNAPSHOT", sourceId, snapshot)));
    }

    private static String digest(List<GenerationDependency> dependencies) {
        if (dependencies.isEmpty()) {
            throw new IllegalArgumentException("artifact dependency set must not be empty");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (GenerationDependency dependency : dependencies) {
                update(digest, dependency.kind());
                update(digest, dependency.id());
                update(digest, dependency.version());
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static int compareUnsignedUtf8(String left, String right) {
        byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
        int commonLength = Math.min(leftBytes.length, rightBytes.length);
        for (int index = 0; index < commonLength; index++) {
            int comparison = Integer.compare(
                    Byte.toUnsignedInt(leftBytes[index]),
                    Byte.toUnsignedInt(rightBytes[index]));
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftBytes.length, rightBytes.length);
    }

    private record LogicalIdentity(String kind, String id) {}
}
