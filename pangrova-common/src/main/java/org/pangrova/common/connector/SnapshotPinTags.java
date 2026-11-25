package org.pangrova.common.connector;

import org.pangrova.common.util.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/** Persistent tag names shared by connectors that pin snapshots for a Pangrova index. */
public final class SnapshotPinTags {
    public static final String PREFIX = "pangrova_index_";
    private static final String VERSIONED_PREFIX = "_v2_";
    private static final HexFormat HEX = HexFormat.of();

    private SnapshotPinTags() {}

    /** Returns the namespace for all snapshots held by this exact index identity. */
    public static String indexPrefix(String indexId) {
        if (indexId == null || indexId.isBlank()) {
            throw new IllegalArgumentException("Index ID is required for snapshot pinning");
        }
        return PREFIX + DigestUtils.sha256Hex(indexId);
    }

    /** Encodes the stable owner and opaque token without imposing connector token syntax. */
    public static String forSnapshot(String indexId, int generation, String attemptId, String snapshotToken) {
        if (attemptId == null || attemptId.isBlank() || snapshotToken == null || snapshotToken.isBlank()) {
            throw new IllegalArgumentException("Attempt and snapshot token are required for snapshot pinning");
        }
        return indexPrefix(indexId) + VERSIONED_PREFIX + generation + "_"
                + encode(attemptId) + "_" + encode(snapshotToken);
    }

    /** Decodes a versioned tag, returning null for legacy or foreign names. */
    public static DecodedSnapshotPin decode(String tag, String indexId) {
        String prefix = indexPrefix(indexId) + VERSIONED_PREFIX;
        if (!tag.startsWith(prefix)) {
            return null;
        }
        DecodedSnapshotPin decoded = decodeAny(tag);
        if (!forSnapshot(indexId, decoded.generation(), decoded.attemptId(), decoded.snapshotToken()).equals(tag)) {
            throw new IllegalStateException("Non-canonical Pangrova snapshot pin tag");
        }
        return decoded;
    }

    /** Decodes the owner-independent payload of a versioned tag for table-wide enumeration. */
    public static DecodedSnapshotPin decodeAny(String tag) {
        int marker = tag.indexOf(VERSIONED_PREFIX);
        if (marker < 0) {
            return null;
        }
        String[] parts = tag.substring(marker + VERSIONED_PREFIX.length()).split("_", -1);
        if (parts.length != 3) {
            throw new IllegalStateException("Malformed Pangrova snapshot pin tag");
        }
        try {
            int generation = Integer.parseInt(parts[0]);
            String attemptId = decode(parts[1]);
            String snapshotToken = decode(parts[2]);
            if (!encode(attemptId).equals(parts[1]) || !encode(snapshotToken).equals(parts[2])) {
                throw new IllegalStateException("Non-canonical Pangrova snapshot pin tag");
            }
            return new DecodedSnapshotPin(generation, attemptId, snapshotToken);
        } catch (IllegalArgumentException invalid) {
            throw new IllegalStateException("Malformed Pangrova snapshot pin tag", invalid);
        }
    }

    /** Reads the decimal token from a pre-owner tag for durable cleanup of existing pins. */
    public static String legacySnapshotToken(String tag, String indexId) {
        String prefix = indexPrefix(indexId) + "_";
        if (!tag.startsWith(prefix) || tag.startsWith(indexPrefix(indexId) + VERSIONED_PREFIX)) {
            return null;
        }
        String token = tag.substring(prefix.length());
        try {
            Long.parseLong(token);
            return token;
        } catch (NumberFormatException invalid) {
            throw new IllegalStateException("Malformed legacy Pangrova snapshot pin tag", invalid);
        }
    }

    /** Hex keeps components delimiter-safe and valid as Paimon tag file names. */
    private static String encode(String value) {
        return HEX.formatHex(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String value) {
        return new String(HEX.parseHex(value), StandardCharsets.UTF_8);
    }

    public record DecodedSnapshotPin(int generation, String attemptId, String snapshotToken) {}
}
