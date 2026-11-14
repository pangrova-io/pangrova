package org.pangrova.persistence.query.api.commit;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;

import org.pangrova.common.api.annotation.Experimental;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import org.pangrova.common.file.CopyFileCommitBinding;

/** Computes the canonical, fragment-order-independent identity of a distributed write commit. */
@Experimental
public final class WriteCommitFingerprint {

    private static final String CONTRACT_VERSION = "pangrova-distributed-write-v4";

    private WriteCommitFingerprint() {
    }

    /**
     * Hashes one fenced write intent and its unordered worker fragments.
     *
     * @param intent immutable identity, exact allocation and authenticated deferred authority
     * @param fragments serialized worker commit fragments
     * @return lowercase SHA-256 identity for retry comparison
     */
    public static String compute(
            Intent intent,
            List<byte[]> fragments) {
        Objects.requireNonNull(intent, "intent must not be null");
        byte[] writeOutputBinding = intent.writeOutputBinding();
        Objects.requireNonNull(writeOutputBinding, "writeOutputBinding must not be null");
        if (writeOutputBinding.length == 0) {
            throw new IllegalArgumentException("writeOutputBinding must not be empty");
        }
        Objects.requireNonNull(fragments, "fragments must not be null");
        List<byte[]> fragmentDigests = fragments.stream()
                .map(fragment -> DigestUtils.sha256().digest(Objects.requireNonNull(fragment, "fragment must not be null")))
                .sorted(WriteCommitFingerprint::compareUnsigned)
                .toList();
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writeString(out, CONTRACT_VERSION);
                writeString(out, intent.executionId());
                writeString(out, intent.tenantId());
                writeString(out, intent.catalogName());
                writeString(out, intent.tableName());
                writeString(out, intent.writeMode());
                out.writeLong(intent.baseSnapshotId());
                writeString(out, intent.principal());
                byte[] scope = intent.authorizationReceipt().authorizationScope();
                out.writeInt(scope.length);
                out.write(scope);
                writeDeclaration(out, intent.authorizationReceipt().declaration());
                out.writeInt(writeOutputBinding.length);
                out.write(writeOutputBinding);
                CopyFileCommitBinding copyFile = intent.copyFile();
                out.writeBoolean(copyFile != null);
                if (copyFile != null) {
                    writeString(out, copyFile.fileKey());
                    out.writeLong(copyFile.loadGeneration());
                    writeString(out, copyFile.claimDigest());
                    writeString(out, copyFile.tableUuid());
                }
                out.writeInt(fragmentDigests.size());
                for (byte[] fragmentDigest : fragmentDigests) {
                    out.write(fragmentDigest);
                }
            }
            return HexFormat.of().formatHex(DigestUtils.sha256().digest(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("Unexpected in-memory fingerprint encoding failure", impossible);
        }
    }

    /** Immutable fingerprint input; this Java aggregate is not an additional transport protocol. */
    public record Intent(String executionId, String tenantId, String catalogName, String tableName,
                         String writeMode, long baseSnapshotId, byte[] writeOutputBinding,
                         String principal, WriteAuthorizationReceipt authorizationReceipt,
                         CopyFileCommitBinding copyFile) {
        /** Copies the exact allocation and authority content before hashing. */
        public Intent {
            if (principal == null || principal.isBlank()) {
                throw new IllegalArgumentException("Write fingerprint requires the real authenticated principal");
            }
            writeOutputBinding = Objects.requireNonNull(writeOutputBinding, "writeOutputBinding").clone();
            Objects.requireNonNull(authorizationReceipt, "authorizationReceipt");
        }

        @Override
        public byte[] writeOutputBinding() { return writeOutputBinding.clone(); }

    }

    private static void writeDeclaration(DataOutputStream output, WriteDeclarationSnapshot declaration)
            throws IOException {
        writeString(output, declaration.table().catalog());
        writeString(output, declaration.table().schema());
        writeString(output, declaration.table().table());
        writeString(output, declaration.schemaSha256());
        writeString(output, declaration.generatedColumnsSha256());
        output.writeBoolean(declaration.hasGeneratedColumns());
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] utf8 = Objects.requireNonNull(value, "fingerprint field must not be null")
                .getBytes(StandardCharsets.UTF_8);
        out.writeInt(utf8.length);
        out.write(utf8);
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        return java.util.Arrays.compareUnsigned(left, right);
    }
}
