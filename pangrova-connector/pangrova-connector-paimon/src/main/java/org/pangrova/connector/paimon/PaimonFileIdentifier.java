package org.pangrova.connector.paimon;

import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.utils.SerializationUtils;
import org.pangrova.common.util.DigestUtils;

import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Cross-language digest of the SDK's complete physical file identity, never of projected values. */
final class PaimonFileIdentifier {
    private PaimonFileIdentifier() {}

    static byte[] digest(FileEntry entry) {
        try {
            var digest = DigestUtils.sha256();
            try (var output = new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
                output.write("pangrova-paimon-file-entry-v1\0".getBytes(StandardCharsets.UTF_8));
                var id = entry.identifier();
                binary(output, SerializationUtils.serializeBinaryRow(id.partition));
                output.writeInt(id.bucket);
                output.writeInt(id.level);
                text(output, id.fileName);
                output.writeInt(id.extraFiles.size());
                for (String extra : id.extraFiles) text(output, extra);
                binary(output, id.embeddedIndex);
                text(output, id.externalPath);
            }
            return digest.digest();
        } catch (IOException impossible) {
            throw new IllegalStateException("Cannot encode physical file identity", impossible);
        }
    }

    private static void text(DataOutputStream output, String text) throws IOException {
        binary(output, text == null ? null : text.getBytes(StandardCharsets.UTF_8));
    }

    private static void binary(DataOutputStream output, byte[] bytes) throws IOException {
        output.writeInt(bytes == null ? -1 : bytes.length);
        if (bytes != null) output.write(bytes);
    }
}
