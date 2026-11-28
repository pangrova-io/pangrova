package org.pangrova.protocol.checksum;

import com.google.protobuf.ByteString;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.protocol.v2.Checksum;

/** Constructs canonical integrity evidence from already selected payload bytes. */
@Internal
public final class ProtocolChecksums {
    private ProtocolChecksums() {}

    /** Hashes the supplied bytes without changing their serialization or ordering. */
    public static Checksum sha256(byte[] bytes) {
        return sha256Value(Sha256Digests.sha256().digest(bytes));
    }

    /** Hashes the immutable plan bytes without making a full-size array copy. */
    public static Checksum sha256(ByteString bytes) {
        var digest = Sha256Digests.sha256();
        for (var buffer : bytes.asReadOnlyByteBufferList()) {
            digest.update(buffer);
        }
        return sha256Value(digest.digest());
    }

    private static Checksum sha256Value(byte[] digest) {
        return Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(digest)).build();
    }
}
