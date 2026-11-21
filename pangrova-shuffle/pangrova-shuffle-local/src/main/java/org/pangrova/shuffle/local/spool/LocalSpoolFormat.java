package org.pangrova.shuffle.local.spool;

import org.pangrova.common.util.DigestUtils;

import java.nio.ByteBuffer;

/** Private, versioned batch framing. Never Java object serialization or a SQL-type codec. */
final class LocalSpoolFormat {
    static final int MAGIC = 0x50475350;
    static final int VERSION = 1;
    static final int END = -1;
    static final int MAX_BATCH_BYTES = 64 * 1024 * 1024;
    static final int DIGEST_BYTES = 32;

    private LocalSpoolFormat() {}

    static byte[] digest(ByteBuffer data) {
        var digest = DigestUtils.sha256();
        digest.update(data.duplicate());
        return digest.digest();
    }
}
