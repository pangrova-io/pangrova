package org.pangrova.storage.s3.object;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real SDK stream/abort contracts; the in-memory body is not external-provider qualification. */
class S3ObjectReadStreamTest {
    @Test
    void partialCloseAbortsOnceWithoutInvokingTheSdkDrainPath() throws IOException {
        var closed = new AtomicInteger();
        var aborted = new AtomicInteger();
        var input = stream(3, closed, aborted);
        assertThat(input.read()).isEqualTo(1);
        input.close();
        input.close();
        assertThat(aborted.get()).isEqualTo(1);
        assertThat(closed.get()).isZero();
        assertThatThrownBy(input::read).isInstanceOf(IOException.class).hasMessageContaining("closed");
    }

    @Test
    void exactLengthConsumptionReturnsTheResponseToItsPoolWithoutAnExtraEofRead() throws IOException {
        var closed = new AtomicInteger();
        var aborted = new AtomicInteger();
        try (var input = stream(3, closed, aborted)) {
            assertThat(input.readNBytes(3)).containsExactly((byte) 1, (byte) 2, (byte) 3);
        }
        assertThat(closed.get()).isEqualTo(1);
        assertThat(aborted.get()).isZero();
    }

    @Test
    void truncatedBodyFailsAndAbortsRatherThanReturningAnUnprovenConnection() throws IOException {
        var closed = new AtomicInteger();
        var aborted = new AtomicInteger();
        try (var input = stream(4, closed, aborted)) {
            assertThatThrownBy(input::readAllBytes).isInstanceOf(IOException.class).hasMessageContaining("declared length");
        }
        assertThat(aborted.get()).isEqualTo(1);
        assertThat(closed.get()).isZero();
    }

    @Test
    void skippedBytesCountAsConsumptionAndResetCannotRewindTheLengthProof() throws IOException {
        var closed = new AtomicInteger();
        var aborted = new AtomicInteger();
        try (var input = stream(3, closed, aborted)) {
            assertThat(input.markSupported()).isFalse();
            assertThatThrownBy(input::reset).isInstanceOf(IOException.class);
            assertThat(input.skip(2)).isEqualTo(2);
            assertThat(input.read()).isEqualTo(3);
        }
        assertThat(closed.get()).isEqualTo(1);
        assertThat(aborted.get()).isZero();
    }

    private static S3ObjectReadStream stream(long length, AtomicInteger closed, AtomicInteger aborted) {
        var body = new ByteArrayInputStream(new byte[]{1, 2, 3}) {
            @Override public void close() {
                closed.incrementAndGet();
                readAllBytes(); // Models the SDK's normal close/drain branch.
            }
        };
        var response = new ResponseInputStream<>(GetObjectResponse.builder().contentLength(length).build(),
                AbortableInputStream.create(body, aborted::incrementAndGet));
        return new S3ObjectReadStream(response, length);
    }
}
