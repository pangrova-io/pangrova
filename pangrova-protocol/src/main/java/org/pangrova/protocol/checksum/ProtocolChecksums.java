package org.pangrova.protocol.checksum;

import com.google.protobuf.ByteString;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.protocol.v2.Checksum;

/** Constructs canonical integrity evidence from already selected payload bytes. */
@Internal
public final class ProtocolChecksums {
    private ProtocolChecksums() {}

    /** Hashes the supplied bytes without changing their serialization or ordering. */
    public static Checksum sha256(byte[] bytes) {
        return Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                .setDigest(ByteString.copyFrom(DigestUtils.sha256().digest(bytes))).build();
    }
}
