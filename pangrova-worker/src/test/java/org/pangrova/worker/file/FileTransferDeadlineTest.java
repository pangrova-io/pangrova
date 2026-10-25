package org.pangrova.worker.file;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileTransferDeadlineTest {
    @Test
    void bytesReturningAfterExpiryAreNotDeliveredAndTheStorageStreamIsClosed() throws IOException {
        var now = new AtomicLong(999);
        var closed = new AtomicBoolean();
        var storage = new ByteArrayInputStream(new byte[]{1, 2, 3}) {
            @Override
            public synchronized int read(byte[] bytes, int offset, int length) {
                int count = super.read(bytes, offset, length);
                now.set(1_000);
                return count;
            }

            @Override
            public void close() throws IOException {
                closed.set(true);
                super.close();
            }
        };
        var delivered = new ByteArrayOutputStream();
        try (var input = new FileTransferDeadline(storage, 1_000, now::get)) {
            assertThatThrownBy(() -> input.transferTo(delivered)).isInstanceOf(IOException.class)
                    .hasMessageContaining("expired");
            assertThat(delivered.size()).isZero();
            assertThat(closed).isTrue();
        }
    }
}
