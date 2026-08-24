package org.pangrova.spi.execution.interop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the {@link NativeBufferHandle} interface contract.
 * Uses a stub implementation to verify ref-counting lifecycle behavior.
 */
class NativeBufferHandleTest {

    /**
     * Stub implementation for testing the NativeBufferHandle contract.
     * Simulates ref-counted native buffer ownership.
     */
    static class StubNativeBufferHandle implements NativeBufferHandle {

        private final long handle;
        private final long address;
        private final long length;
        private final AtomicInteger refCount = new AtomicInteger(1);
        private final AtomicBoolean nativeReleased = new AtomicBoolean(false);
        private final NativeBufferReleaser releaser;

        StubNativeBufferHandle(long handle, long address, long length,
                               NativeBufferReleaser releaser) {
            this.handle = handle;
            this.address = address;
            this.length = length;
            this.releaser = releaser;
        }

        @Override
        public long nativeHandle() {
            checkAlive();
            return handle;
        }

        @Override
        public long bufferAddress() {
            checkAlive();
            return address;
        }

        @Override
        public long bufferLength() {
            checkAlive();
            return length;
        }

        @Override
        public int refCount() {
            return refCount.get();
        }

        @Override
        public void retain() {
            int current = refCount.get();
            if (current <= 0) {
                throw new IllegalStateException(
                        "Cannot retain: handle already released");
            }
            if (!refCount.compareAndSet(current, current + 1)) {
                // Retry for concurrent access
                retain();
            }
        }

        @Override
        public void release() {
            int current = refCount.get();
            if (current <= 0) {
                throw new IllegalStateException(
                        "Cannot release: reference count already 0");
            }
            if (!refCount.compareAndSet(current, current - 1)) {
                // Retry for concurrent access
                release();
                return;
            }
            if (current - 1 == 0) {
                nativeReleased.set(true);
                releaser.releaseNativeBuffer(handle);
            }
        }

        @Override
        public NativeBufferOwnership ownership() {
            return NativeBufferOwnership.NATIVE_OWNED_HANDLE;
        }

        boolean isNativeReleased() {
            return nativeReleased.get();
        }

        private void checkAlive() {
            if (refCount.get() <= 0) {
                throw new IllegalStateException("Handle has been released");
            }
        }
    }

    @Test
    void newHandleHasRefCountOne() {
        StubNativeBufferHandle handle = createHandle(0x1000L, 0x2000L, 4096);
        assertThat(handle.refCount()).isEqualTo(1);
    }

    @Test
    void retainIncrementsRefCount() {
        StubNativeBufferHandle handle = createHandle(0x1000L, 0x2000L, 4096);
        handle.retain();
        assertThat(handle.refCount()).isEqualTo(2);
        handle.retain();
        assertThat(handle.refCount()).isEqualTo(3);
        // Cleanup
        handle.release();
        handle.release();
        handle.release();
    }

    @Test
    void releaseDecrementsRefCount() {
        StubNativeBufferHandle handle = createHandle(0x1000L, 0x2000L, 4096);
        handle.retain(); // refCount=2
        handle.release(); // refCount=1
        assertThat(handle.refCount()).isEqualTo(1);
        assertThat(handle.isNativeReleased()).isFalse();
    }

    @Test
    void finalReleaseInvokesNativeReleaser() {
        AtomicBoolean released = new AtomicBoolean(false);
        StubNativeBufferHandle handle = new StubNativeBufferHandle(
                0x1000L, 0x2000L, 4096,
                nativeHandle -> released.set(true));

        handle.release();
        assertThat(handle.refCount()).isEqualTo(0);
        assertThat(released.get()).isTrue();
        assertThat(handle.isNativeReleased()).isTrue();
    }

    @Test
    void doubleReleaseThrows() {
        StubNativeBufferHandle handle = createHandle(0x1000L, 0x2000L, 4096);
        handle.release();

        assertThatThrownBy(handle::release)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already 0");
    }

    @Test
    void retainAfterFullReleaseThrows() {
        StubNativeBufferHandle handle = createHandle(0x1000L, 0x2000L, 4096);
        handle.release();

        assertThatThrownBy(handle::retain)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already released");
    }

    @Test
    void accessorsWorkWhileAlive() {
        StubNativeBufferHandle handle = createHandle(0xABCDL, 0x5000L, 8192);
        assertThat(handle.nativeHandle()).isEqualTo(0xABCDL);
        assertThat(handle.bufferAddress()).isEqualTo(0x5000L);
        assertThat(handle.bufferLength()).isEqualTo(8192L);
        handle.release();
    }

    @Test
    void accessorsThrowAfterRelease() {
        StubNativeBufferHandle handle = createHandle(0x1000L, 0x2000L, 4096);
        handle.release();

        assertThatThrownBy(handle::nativeHandle)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(handle::bufferAddress)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(handle::bufferLength)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void ownershipIsNativeOwnedHandle() {
        StubNativeBufferHandle handle = createHandle(0x1000L, 0x2000L, 4096);
        assertThat(handle.ownership())
                .isEqualTo(NativeBufferOwnership.NATIVE_OWNED_HANDLE);
        handle.release();
    }

    @Test
    void closeEquivalentToRelease() {
        AtomicBoolean released = new AtomicBoolean(false);
        StubNativeBufferHandle handle = new StubNativeBufferHandle(
                0x1000L, 0x2000L, 4096,
                nativeHandle -> released.set(true));

        handle.close(); // should invoke release()
        assertThat(handle.refCount()).isEqualTo(0);
        assertThat(released.get()).isTrue();
    }

    @Test
    void tryWithResourcesReleasesHandle() {
        AtomicBoolean released = new AtomicBoolean(false);
        try (StubNativeBufferHandle handle = new StubNativeBufferHandle(
                0x1000L, 0x2000L, 4096,
                nativeHandle -> released.set(true))) {
            assertThat(handle.nativeHandle()).isEqualTo(0x1000L);
        }
        assertThat(released.get()).isTrue();
    }

    @Test
    void releaserReceivesCorrectHandle() {
        long expectedHandle = 0xDEAD_BEEFL;
        long[] capturedHandle = {0};
        StubNativeBufferHandle handle = new StubNativeBufferHandle(
                expectedHandle, 0x2000L, 4096,
                nativeHandle -> capturedHandle[0] = nativeHandle);

        handle.release();
        assertThat(capturedHandle[0]).isEqualTo(expectedHandle);
    }

    @Test
    void multipleRetainAndReleaseCycle() {
        AtomicInteger releaseCount = new AtomicInteger(0);
        StubNativeBufferHandle handle = new StubNativeBufferHandle(
                0x1000L, 0x2000L, 4096,
                nativeHandle -> releaseCount.incrementAndGet());

        // Consumer A retains
        handle.retain(); // refCount=2
        // Consumer B retains
        handle.retain(); // refCount=3

        // Original holder releases
        handle.release(); // refCount=2, no native release
        assertThat(handle.refCount()).isEqualTo(2);
        assertThat(releaseCount.get()).isEqualTo(0);

        // Consumer A releases
        handle.release(); // refCount=1, no native release
        assertThat(handle.refCount()).isEqualTo(1);
        assertThat(releaseCount.get()).isEqualTo(0);

        // Consumer B releases (final)
        handle.release(); // refCount=0, native release
        assertThat(handle.refCount()).isEqualTo(0);
        assertThat(releaseCount.get()).isEqualTo(1);
    }

    @Test
    void isAutoCloseable() {
        assertThat(AutoCloseable.class)
                .isAssignableFrom(NativeBufferHandle.class);
    }

    private static StubNativeBufferHandle createHandle(
            long nativeHandle, long address, long length) {
        return new StubNativeBufferHandle(
                nativeHandle, address, length,
                handle -> { /* no-op releaser */ });
    }
}
