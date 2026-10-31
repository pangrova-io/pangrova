package org.pangrova.file.api.upload;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PresignedObjectUrlTest {
    @Test
    void keepsProviderExpiryAndRedactsBearerCredentials() {
        var signed = new PresignedObjectUrl("https://storage.invalid/key?X-Amz-Signature=private", 12_345);
        assertThat(signed.expiresAtEpochMillis()).isEqualTo(12_345);
        assertThat(signed.url()).contains("X-Amz-Signature");
        assertThat(signed.toString()).doesNotContain("storage.invalid", "X-Amz", "private");
        assertThatThrownBy(() -> new PresignedObjectUrl("", 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PresignedObjectUrl("url", 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
