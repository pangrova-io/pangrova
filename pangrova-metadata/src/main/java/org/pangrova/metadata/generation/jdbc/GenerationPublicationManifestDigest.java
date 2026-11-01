package org.pangrova.metadata.generation.jdbc;

import org.pangrova.spi.generation.job.GenerationOutput;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

/** Canonical content digest of a published index artifact generation. */
final class GenerationPublicationManifestDigest {

    private GenerationPublicationManifestDigest() {}

    static String compute(
            JdbcIndexGenerationPublicationStore.JobRow job,
            List<GenerationOutput> outputs) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            update(digest, job.tenantId());
            update(digest, job.artifactId());
            update(digest, job.artifactType());
            update(digest, job.generation());
            update(digest, job.sourceId());
            update(digest, job.sourceSnapshot());
            update(digest, job.executionKind().name());
            update(digest, job.dependencySet().sha256());
            update(digest, job.emptyPartitionPolicy().name());
            update(digest, job.formatId());
            update(digest, Integer.toString(job.formatMajor()));
            update(digest, Integer.toString(job.formatMinor()));
            update(digest, job.schemaSha256().toLowerCase(Locale.ROOT));
            update(digest, job.writerBuildId());
            outputs.stream().sorted(Comparator
                            .comparing(GenerationOutput::sourceObjectId)
                            .thenComparingInt(GenerationOutput::ordinal)
                            .thenComparing(GenerationOutput::objectId))
                    .forEach(output -> updateOutput(digest, output));
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static void updateOutput(MessageDigest digest, GenerationOutput output) {
        update(digest, output.sourceObjectId());
        update(digest, output.objectRole());
        update(digest, output.objectId());
        update(digest, output.opaqueUri());
        update(digest, output.checksumSha256().toLowerCase(Locale.ROOT));
        update(digest, Long.toString(output.sizeBytes()));
        update(digest, Long.toString(output.rowCount()));
        update(digest, Integer.toString(output.ordinal()));
        update(digest, output.scope().name());
        update(digest, output.encryptionKeyRef());
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
