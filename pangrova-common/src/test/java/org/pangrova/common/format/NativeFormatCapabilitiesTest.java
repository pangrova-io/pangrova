package org.pangrova.common.format;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NativeFormatCapabilitiesTest {

    @AfterEach
    void resetState() {
        NativeFormatCapabilities.setAvailable(true);
        System.clearProperty("pangrova.native-format.available");
    }

    @Test
    void defaultsToAvailable() {
        assertThat(NativeFormatCapabilities.isAvailable()).isTrue();
    }

    @Test
    void setAvailableUpdatesState() {
        NativeFormatCapabilities.setAvailable(false);
        assertThat(NativeFormatCapabilities.isAvailable()).isFalse();

        NativeFormatCapabilities.setAvailable(true);
        assertThat(NativeFormatCapabilities.isAvailable()).isTrue();
    }

    @Test
    void unavailableBlocksNativeFormatOperations() {
        NativeFormatCapabilities.setAvailable(false);
        assertThat(NativeFormatCapabilities.isAvailable()).isFalse();
    }
}
