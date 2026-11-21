package org.pangrova.protocol.capability;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CapabilityIdentitiesTest {
    @Test
    void semanticIdentityAndOptionalBuildPinHaveDistinctRoles() {
        var required = BuiltInCapabilityCatalog.crossEncoderRerankPlanRef();
        var advertised = required.toBuilder().setImplementationBuildId("native-a").build();
        assertThat(CapabilityIdentities.matches(advertised, required)).isTrue();
        assertThat(CapabilityIdentities.matches(advertised, advertised)).isTrue();
        for (var different : java.util.List.of(
                required.toBuilder().setCapabilityId("another").build(),
                required.toBuilder().setSemanticVersion("another").build(),
                required.toBuilder().setSignatureSha256(ByteString.copyFrom(new byte[32])).build(),
                required.toBuilder().setImplementationBuildId("native-b").build())) {
            assertThat(CapabilityIdentities.matches(advertised, different)).isFalse();
        }
        assertThat(CapabilityIdentities.matches(required, advertised)).isFalse();
    }
}
