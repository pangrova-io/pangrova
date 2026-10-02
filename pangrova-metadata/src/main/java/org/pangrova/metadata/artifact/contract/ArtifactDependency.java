package org.pangrova.metadata.artifact.contract;

/** One version-pinned input to an immutable artifact generation. */
public record ArtifactDependency(String kind, String id, String version) {

    public ArtifactDependency {
        kind = requireText(kind, "kind");
        id = requireText(id, "id");
        version = requireText(version, "version");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("artifact dependency " + name + " must not be blank");
        }
        for (int offset = 0; offset < value.length();) {
            char first = value.charAt(offset);
            if (Character.isHighSurrogate(first)) {
                if (offset + 1 >= value.length()
                        || !Character.isLowSurrogate(value.charAt(offset + 1))) {
                    throw invalidUnicode(name);
                }
            } else if (Character.isLowSurrogate(first)) {
                throw invalidUnicode(name);
            }
            int codePoint = value.codePointAt(offset);
            if (codePoint <= 0x1f || codePoint >= 0x7f && codePoint <= 0x9f) {
                throw new IllegalArgumentException(
                        "artifact dependency " + name
                                + " must not contain Unicode control characters");
            }
            offset += Character.charCount(codePoint);
        }
        return value;
    }

    private static IllegalArgumentException invalidUnicode(String name) {
        return new IllegalArgumentException(
                "artifact dependency " + name
                        + " must contain valid Unicode scalar values");
    }
}
