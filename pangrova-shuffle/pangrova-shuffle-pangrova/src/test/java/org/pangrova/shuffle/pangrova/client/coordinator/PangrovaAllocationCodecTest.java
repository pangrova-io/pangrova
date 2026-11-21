package org.pangrova.shuffle.pangrova.client.coordinator;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ShuffleAccessCapability;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.coordinator.ShuffleLifecycleEvidence.Allocation;
import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PangrovaAllocationCodecTest {
    private static final ShuffleServiceAddress ADDRESS = new ShuffleServiceAddress("localhost", 9011, "shuffle");
    private static final List<ShuffleServerEndpoint> ROUTES = List.of(
            new ShuffleServerEndpoint("worker-a", 9011, 0, 2, "shuffle-a"),
            new ShuffleServerEndpoint("worker-b", 9011, 2, 3, "shuffle-b"));

    @Test
    void decodesOnlyTheExactProviderVersionAllocationAndCoverage() {
        var descriptor = PangrovaAllocationCodec.encode("allocation", ADDRESS, ROUTES, 3);
        assertThat(PangrovaAllocationCodec.decode(new Allocation("allocation", descriptor), 3)).isEqualTo(ADDRESS);
        assertThatThrownBy(() -> PangrovaAllocationCodec.decode(new Allocation("other", descriptor), 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PangrovaAllocationCodec.decode(new Allocation("allocation", descriptor), 4))
                .isInstanceOf(IllegalArgumentException.class);
        for (var invalid : List.of(ShuffleProviderDescriptor.of("other", 1, descriptor.payload()),
                ShuffleProviderDescriptor.of("pangrova", 2, descriptor.payload()),
                ShuffleProviderDescriptor.of("pangrova", 1, new byte[]{-1}))) {
            assertThatThrownBy(() -> PangrovaAllocationCodec.decode(new Allocation("allocation", invalid), 3))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsExtraWireFieldsEvenWithAValidCredentialAndCoverage() throws Exception {
        var descriptor = PangrovaAllocationCodec.encode("allocation", ADDRESS, ROUTES, 3);
        var secret = RegisterShuffleResponse.parseFrom(descriptor.payload()).toBuilder().setReadCapability(
                ShuffleAccessCapability.newBuilder().setToken(ByteString.copyFromUtf8("secret"))).build();
        var altered = ShuffleProviderDescriptor.of("pangrova", 1, secret.toByteArray());
        assertThatThrownBy(() -> PangrovaAllocationCodec.decode(new Allocation("allocation", altered), 3))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Noncanonical");
    }

    @Test
    void boundsDecodedAndEncodedInputAndOwnsItsBytes() {
        byte[] bytes = new byte[ShuffleProviderDescriptor.MAX_BYTES];
        var allowed = ShuffleProviderDescriptor.of("test", 1, bytes);
        bytes[0] = 7;
        allowed.payload()[0] = 3;
        assertThat(allowed.payload()[0]).isZero();
        byte[] oversized = new byte[ShuffleProviderDescriptor.MAX_BYTES + 1];
        assertThatThrownBy(() -> ShuffleProviderDescriptor.of("test", 1, oversized))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ShuffleProviderDescriptor("test", 1,
                java.util.Base64.getEncoder().encodeToString(oversized))).isInstanceOf(IllegalArgumentException.class);
        for (String payload : List.of("", "AQ", "A+==", "AQI@")) {
            assertThatThrownBy(() -> new ShuffleProviderDescriptor("test", 1, payload))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
