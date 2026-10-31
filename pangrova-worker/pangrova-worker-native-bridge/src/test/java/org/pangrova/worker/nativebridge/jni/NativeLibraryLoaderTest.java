package org.pangrova.worker.nativebridge.jni;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NativeLibraryLoaderTest {

    @Test
    void packagedNativeLibraryIsAvailableAndLoadable() {
        assertThat(NativeLibraryLoader.isAvailable()).isTrue();
        assertThat(NativeLibraryLoader.isFunctional()).isTrue();
    }
}