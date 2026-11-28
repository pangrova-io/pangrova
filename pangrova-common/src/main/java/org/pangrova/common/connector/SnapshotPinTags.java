package org.pangrova.common.connector;

import org.pangrova.common.crypto.Sha256Digests;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Predicate;

/** Persistent tag names shared by connectors that pin snapshots for a Pangrova index. */
public final class SnapshotPinTags {
    public static final String PREFIX = "pangrova_index_";
    private static final String V2 = "_v2_";
    private static final String V3 = "_v3_";
    private static final HexFormat HEX = HexFormat.of();

    private SnapshotPinTags() {}

    /** Returns the namespace for all snapshots held by this exact index identity. */
    public static String indexPrefix(String indexId) {
        if (indexId == null || indexId.isBlank()) {
            throw new IllegalArgumentException("Index ID is required for snapshot pinning");
        }
        return PREFIX + Sha256Digests.sha256Hex(indexId);
    }

    /** Writes only the v3 format, with delimiter-safe, filesystem-compatible UTF-8 hex. */
    public static String forSnapshot(String indexId, int generation, String attemptId, String snapshotToken) {
        return name(indexId, generation, attemptId, snapshotToken, V3, false);
    }

    /**
     * Resolves every existing encoding of the exact pin, verifying persisted identity before
     * returning names to the storage provider. The predicate also validates the physical snapshot.
     */
    public static List<String> matchingNames(String indexId, int generation, String attemptId, String snapshotToken,
                                             Predicate<String> validateExisting) {
        List<String> names = List.of(forSnapshot(indexId, generation, attemptId, snapshotToken),
                name(indexId, generation, attemptId, snapshotToken, V2, true),
                name(indexId, generation, attemptId, snapshotToken, V2, false)).stream().distinct()
                .filter(validateExisting)
                .toList();
        names.forEach(tag -> requireExact(tag, indexId, generation, attemptId, snapshotToken));
        return names;
    }

    private static String name(String indexId, int generation, String attemptId, String snapshotToken,
                               String version, boolean base64) {
        String index = indexPrefix(indexId);
        if (generation < 0 || attemptId == null || attemptId.isBlank()
                || snapshotToken == null || snapshotToken.isBlank()) {
            throw new IllegalArgumentException(
                    "Non-negative generation, attempt and snapshot token are required for snapshot pinning");
        }
        return index + version + generation + "_" + encode(attemptId, base64) + "_" + encode(snapshotToken, base64);
    }

    /** Decodes a versioned tag, returning null for legacy or foreign names. */
    public static DecodedSnapshotPin decode(String tag, String indexId) {
        if (!tag.startsWith(indexPrefix(indexId) + "_")) {
            return null;
        }
        return decodeAny(tag);
    }

    /** Decodes the owner-independent payload of a versioned tag for table-wide enumeration. */
    public static DecodedSnapshotPin decodeAny(String tag) {
        int marker = PREFIX.length() + 64;
        if (!tag.startsWith(PREFIX) || tag.length() <= marker
                || !tag.substring(PREFIX.length(), marker).matches("[0-9a-f]{64}")) {
            return null;
        }
        if (tag.startsWith(V3, marker)) {
            return requireDecoded(parse(tag.substring(marker + V3.length()), false));
        }
        if (tag.startsWith(V2, marker)) {
            String payload = tag.substring(marker + V2.length());
            DecodedSnapshotPin base64 = parse(payload, true);
            DecodedSnapshotPin hex = parse(payload, false);
            if (base64 != null && hex != null && !base64.equals(hex)) {
                throw new IllegalStateException("Ambiguous Pangrova v2 snapshot pin tag; reconcile before cleanup");
            }
            return requireDecoded(base64 != null ? base64 : hex);
        }
        if (tag.startsWith("_v", marker)) {
            throw new IllegalStateException("Unsupported Pangrova snapshot pin tag version");
        }
        return null;
    }

    private static DecodedSnapshotPin requireDecoded(DecodedSnapshotPin value) {
        if (value == null) {
            throw new IllegalStateException("Malformed or non-canonical Pangrova snapshot pin tag");
        }
        return value;
    }

    private static DecodedSnapshotPin parse(String payload, boolean base64) {
        String[] parts = payload.split("_", -1);
        if (parts.length != 3) {
            return null;
        }
        try {
            int generation = Integer.parseInt(parts[0]);
            String attemptId = decode(parts[1], base64);
            String snapshotToken = decode(parts[2], base64);
            if (generation < 0 || !Integer.toString(generation).equals(parts[0])
                    || attemptId.isBlank() || snapshotToken.isBlank()
                    || !encode(attemptId, base64).equals(parts[1]) || !encode(snapshotToken, base64).equals(parts[2])) {
                return null;
            }
            return new DecodedSnapshotPin(generation, attemptId, snapshotToken);
        } catch (IllegalArgumentException invalid) {
            return null;
        }
    }

    /** Verifies the persisted encoding before an exact candidate name may be used. */
    private static void requireExact(String tag, String indexId, int generation,
                                     String attemptId, String snapshotToken) {
        if (!new DecodedSnapshotPin(generation, attemptId, snapshotToken).equals(decode(tag, indexId))) {
            throw new IllegalStateException("Pangrova snapshot pin tag differs from its owner or token");
        }
    }

    /** Reads the decimal token from a pre-owner tag for durable cleanup of existing pins. */
    public static String legacySnapshotToken(String tag, String indexId) {
        String prefix = indexPrefix(indexId) + "_";
        if (!tag.startsWith(prefix) || decode(tag, indexId) != null) {
            return null;
        }
        String token = tag.substring(prefix.length());
        try {
            if (!Long.toString(Long.parseLong(token)).equals(token)) {
                throw new NumberFormatException("non-canonical legacy snapshot");
            }
            return token;
        } catch (NumberFormatException invalid) {
            throw new IllegalStateException("Malformed legacy Pangrova snapshot pin tag", invalid);
        }
    }

    private static String encode(String value, boolean base64) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (!new String(bytes, StandardCharsets.UTF_8).equals(value)) {
            throw new IllegalArgumentException("Snapshot pin components must be valid UTF-8");
        }
        return base64 ? Base64.getEncoder().encodeToString(bytes) : HEX.formatHex(bytes);
    }

    private static String decode(String value, boolean base64) {
        return new String(base64 ? Base64.getDecoder().decode(value) : HEX.parseHex(value), StandardCharsets.UTF_8);
    }

    public record DecodedSnapshotPin(int generation, String attemptId, String snapshotToken) {}
}
