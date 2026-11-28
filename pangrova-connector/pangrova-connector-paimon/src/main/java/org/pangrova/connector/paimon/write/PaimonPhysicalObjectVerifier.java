package org.pangrova.connector.paimon.write;

import org.pangrova.common.crypto.Sha256Digests;

import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.pangrova.connector.api.metadata.NativePhysicalFileReceipt;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;

/** Checks immutable object bytes through the catalog SDK FileIO, without decoding any rows. */
final class PaimonPhysicalObjectVerifier {
    private PaimonPhysicalObjectVerifier() {}

    static void verify(FileStoreTable table, NativePhysicalFileReceipt receipt) {
        var path = new Path(receipt.allocation().location().opaqueLocation());
        var expected = receipt.metrics();
        try (var input = table.fileIO().newInputStream(path)) {
            var status = table.fileIO().getFileStatus(path);
            if (status.isDir() || status.getLen() != expected.sizeBytes()) {
                throw new IllegalArgumentException("Physical object size differs from its closed-file receipt");
            }
            input.seek(expected.sizeBytes() - 8);
            byte[] footer = input.readNBytes(8);
            verifyFooter(footer, receipt);
            input.seek(0);
            var digest = Sha256Digests.sha256();
            byte[] buffer = new byte[64 * 1024];
            long bytes = 0;
            for (int count; (count = input.read(buffer)) != -1;) {
                if (count == 0) throw new IOException("SDK object reader made no progress");
                bytes = Math.addExact(bytes, count);
                if (bytes > expected.sizeBytes()) {
                    throw new IllegalArgumentException("Physical object grew beyond its closed-file receipt");
                }
                digest.update(buffer, 0, count);
            }
            if (bytes != expected.sizeBytes() || !MessageDigest.isEqual(digest.digest(), receipt.objectSha256())) {
                throw new IllegalArgumentException("Physical object checksum differs from its closed-file receipt");
            }
        } catch (IOException failure) {
            throw new IllegalStateException("SDK physical object verification failed", failure);
        }
    }

    private static void verifyFooter(byte[] footer, NativePhysicalFileReceipt receipt) {
        if (footer.length != 8 || footer[4] != 'L' || footer[5] != 'A' || footer[6] != 'N' || footer[7] != 'C') {
            throw new IllegalArgumentException("Physical object has no valid closed Lance footer");
        }
        var version = ByteBuffer.wrap(footer).order(ByteOrder.LITTLE_ENDIAN);
        int major = Short.toUnsignedInt(version.getShort());
        int minor = Short.toUnsignedInt(version.getShort());
        if (major != receipt.metrics().encodingMajor() || minor != receipt.metrics().encodingMinor()) {
            throw new IllegalArgumentException("Physical object encoding differs from its actual footer receipt");
        }
    }
}
