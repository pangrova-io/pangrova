package org.pangrova.file.api.copy;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CopySourceEndpointPolicyTest {
    @Test
    void productionRequiresExplicitCredentialFreeHttpsOrigin() {
        assertThat(CopySourceEndpointPolicy.require("https://s3.example:443/", false))
                .isEqualTo(URI.create("https://s3.example"));
        for (String endpoint : new String[]{"", "http://127.0.0.1:9000",
                "http://remote.example", "https://user:secret@s3.example",
                "https://s3.example/other", "https://s3.example/?token=secret"}) {
            assertThatThrownBy(() -> CopySourceEndpointPolicy.require(endpoint, false))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageNotContaining("secret");
        }
    }

    @Test
    void explicitTestPolicyAcceptsOnlyLocalHttp() {
        assertThat(CopySourceEndpointPolicy.require("http://localhost:9000", true))
                .isEqualTo(URI.create("http://localhost:9000"));
        assertThat(CopySourceEndpointPolicy.require("http://127.0.0.1:9000", true))
                .isEqualTo(URI.create("http://127.0.0.1:9000"));
        assertThatThrownBy(() -> CopySourceEndpointPolicy.require("http://remote.example", true))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
