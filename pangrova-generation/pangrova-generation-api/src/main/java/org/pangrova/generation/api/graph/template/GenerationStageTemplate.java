package org.pangrova.generation.api.graph.template;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.common.api.annotation.Internal;

import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Versioned, immutable wire-neutral stage bindings and logical result types; never provider credentials. */
@Internal
public record GenerationStageTemplate(int version, String bindingsBase64, String bindingsSha256,
        List<String> resultTypesBase64, List<PublicationRead> publicationReads) {
    public static final int FORMAT_VERSION = 2;
    private static final int MAX_TEMPLATE_REFERENCES = 4096;
    private static final int MAX_ENCODED_TEMPLATE_BYTES = 24 * 1024 * 1024;

    public GenerationStageTemplate {
        if (version != FORMAT_VERSION) {
            throw new IllegalArgumentException("Durable stage template requires format 2; explicitly rebuild old jobs");
        }
        byte[] bindings = decode(bindingsBase64, true);
        if (!DigestUtils.matchesSha256(bindings, bindingsSha256)) {
            throw new IllegalArgumentException("Stage binding template checksum differs");
        }
        resultTypesBase64 = List.copyOf(resultTypesBase64);
        publicationReads = publicationReads.stream().sorted(Comparator.comparing(PublicationRead::artifactId)
                .thenComparing(PublicationRead::generation).thenComparing(PublicationRead::sourceId)
                .thenComparing(PublicationRead::sourceSnapshot)).toList();
        if (resultTypesBase64.size() > MAX_TEMPLATE_REFERENCES
                || publicationReads.size() > MAX_TEMPLATE_REFERENCES
                || publicationReads.stream().map(PublicationRead::identity).distinct().count()
                        != publicationReads.size()) {
            throw new IllegalArgumentException("Stage template has oversized or duplicate immutable references");
        }
        long byteCount = bindings.length;
        for (String value : resultTypesBase64) byteCount = Math.addExact(byteCount, decode(value, false).length);
        if (byteCount > 16 * 1024 * 1024) {
            throw new IllegalArgumentException("Stage template exceeds its metadata byte budget");
        }
    }

    /** Encodes actual server-compiled canonical bindings and indexed logical types without protocol dependencies. */
    public static GenerationStageTemplate of(byte[] bindings, List<byte[]> resultTypes, List<PublicationRead> reads) {
        Objects.requireNonNull(bindings, "bindings");
        return new GenerationStageTemplate(FORMAT_VERSION, Base64.getEncoder().encodeToString(bindings),
                DigestUtils.sha256Hex(bindings), resultTypes.stream()
                        .map(bytes -> Base64.getEncoder().encodeToString(bytes)).toList(), reads);
    }

    public byte[] bindingBytes() { return decode(bindingsBase64, true); }
    public List<byte[]> resultTypeBytes() {
        return resultTypesBase64.stream().map(value -> decode(value, false)).toList();
    }

    /** Exact published input pointer; task-local physical object metadata is loaded on the claim connection. */
    public record PublicationRead(String artifactId, String generation, String sourceId,
            String sourceSnapshot, String manifestSha256) {
        public PublicationRead {
            for (String value : List.of(artifactId, generation, sourceId, sourceSnapshot)) {
                if (value.isBlank()) throw new IllegalArgumentException("Published stage input identity must be exact");
            }
            if (!DigestUtils.isSha256Hex(manifestSha256)) {
                throw new IllegalArgumentException("Published stage input requires one exact manifest SHA-256");
            }
        }

        private List<String> identity() { return List.of(artifactId, generation, sourceId, sourceSnapshot); }
    }

    private static byte[] decode(String encoded, boolean emptyAllowed) {
        Objects.requireNonNull(encoded, "Canonical stage template is absent; explicitly rebuild old jobs");
        if (encoded.length() > MAX_ENCODED_TEMPLATE_BYTES) {
            throw new IllegalArgumentException("Stage template encoding is oversized");
        }
        byte[] bytes = Base64.getDecoder().decode(encoded);
        if ((!emptyAllowed && bytes.length == 0) || bytes.length > 16 * 1024 * 1024
                || !Base64.getEncoder().encodeToString(bytes).equals(encoded)) {
            throw new IllegalArgumentException("Stage template bytes are empty, oversized or noncanonical");
        }
        return bytes;
    }
}
