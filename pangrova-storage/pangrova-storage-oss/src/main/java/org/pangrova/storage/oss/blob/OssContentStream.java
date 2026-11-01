package org.pangrova.storage.oss.blob;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;

import org.pangrova.common.concurrent.NotThreadSafe;
import org.pangrova.storage.api.blob.BlobInputStream;
import org.pangrova.storage.api.blob.BlobObjectId;

/** Validates consumed bytes and aborts failures/partial closes without draining a rejected remote body. */
@NotThreadSafe
final class OssContentStream extends InputStream {
    private final BlobInputStream body;
    private final BlobObjectId object;
    private final MessageDigest sha256 = OssPublicationDraft.digest("SHA-256");
    private long count;
    private boolean closed;
    private IOException failure;

    OssContentStream(BlobInputStream body, BlobObjectId object) throws IOException {
        this.body = body;
        this.object = object;
        verifyComplete();
    }

    @Override
    public int read() throws IOException {
        requireOpen();
        if (count == object.size()) return -1;
        int value = body.read();
        if (value == -1) throw fail("OSS object body is truncated");
        sha256.update((byte) value);
        count++;
        verifyComplete();
        return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        Objects.checkFromIndexSize(offset, length, buffer.length);
        requireOpen();
        if (length == 0) return 0;
        if (count == object.size()) return -1;
        int read = body.read(buffer, offset, (int) Math.min(length, object.size() - count));
        if (read == -1) throw fail("OSS object body is truncated");
        sha256.update(buffer, offset, read);
        count += read;
        verifyComplete();
        return read;
    }

    private void verifyComplete() throws IOException {
        if (count == object.size() && !HexFormat.of().formatHex(sha256.digest()).equals(object.checksum())) {
            throw fail("OSS object body does not match its SHA-256");
        }
    }

    private IOException fail(String message) {
        failure = new IOException(message);
        try {
            body.abort();
        } catch (RuntimeException | Error cleanup) {
            failure.addSuppressed(cleanup);
        }
        return failure;
    }

    private void requireOpen() throws IOException {
        if (failure != null) throw failure;
        if (closed) throw new IOException("OSS object stream is closed");
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        if (count != object.size() || failure != null) body.abort();
        else body.close();
    }
}
