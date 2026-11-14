package org.pangrova.shuffle.pangrova.client.coordinator;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.v2.ShuffleServiceEndpoint;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.shuffle.api.ShuffleServerEndpoint;
import org.pangrova.shuffle.api.ShuffleServiceAddress;
import org.pangrova.shuffle.api.coordinator.ShuffleLifecycleEvidence.Allocation;
import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;

import java.util.List;

/** Version one stores only allocation ID and canonical routes. Access grants are reissued by the service. */
final class PangrovaAllocationCodec {
    private static final int VERSION = 1;
    private PangrovaAllocationCodec() {}

    static ShuffleProviderDescriptor encode(String id, ShuffleServiceAddress address,
            List<ShuffleServerEndpoint> endpoints, int partitions) {
        var wire = RegisterShuffleResponse.newBuilder().setShuffleId(id)
                .setCommitAddress(org.pangrova.protocol.v2.ShuffleServiceAddress.newBuilder()
                        .setHost(address.host()).setPort(address.port()).setTlsIdentity(address.tlsIdentity()));
        int next = 0;
        for (var endpoint : endpoints) {
            if (endpoint.partitionStart() != next || endpoint.partitionEnd() > partitions) {
                throw new IllegalArgumentException("Shuffle allocation descriptor has conflicting partition routes");
            }
            wire.addEndpoints(ShuffleServiceEndpoint.newBuilder().setHost(endpoint.host()).setPort(endpoint.port())
                    .setTlsIdentity(endpoint.tlsIdentity()).setPartitionStart(next).setPartitionEnd(endpoint.partitionEnd()));
            next = endpoint.partitionEnd();
        }
        if (id.isBlank() || partitions <= 0 || next != partitions) {
            throw new IllegalArgumentException("Shuffle allocation descriptor has incomplete identity or routes");
        }
        var value = wire.build();
        if (value.getSerializedSize() > ShuffleProviderDescriptor.MAX_BYTES) {
            throw new IllegalArgumentException("Shuffle allocation descriptor exceeds size limit");
        }
        return ShuffleProviderDescriptor.of(PangrovaShuffleCoordinatorFactory.PROVIDER_ID, VERSION, value.toByteArray());
    }

    static ShuffleServiceAddress decode(Allocation allocation, int partitions) {
        var descriptor = allocation.descriptor();
        if (!PangrovaShuffleCoordinatorFactory.PROVIDER_ID.equals(descriptor.providerId()) || descriptor.version() != VERSION) {
            throw new IllegalArgumentException("Unsupported shuffle allocation descriptor provider or version");
        }
        try {
            var wire = RegisterShuffleResponse.parseFrom(descriptor.payload());
            var route = wire.getCommitAddress();
            var address = new ShuffleServiceAddress(route.getHost(), route.getPort(), route.getTlsIdentity());
            var endpoints = wire.getEndpointsList().stream().map(endpoint -> new ShuffleServerEndpoint(
                    endpoint.getHost(), endpoint.getPort(), endpoint.getPartitionStart(), endpoint.getPartitionEnd(),
                    endpoint.getTlsIdentity())).toList();
            if (!wire.getShuffleId().equals(allocation.shuffleId())
                    || !encode(allocation.shuffleId(), address, endpoints, partitions).equals(descriptor)) {
                throw new IllegalArgumentException("Noncanonical or mismatched shuffle allocation descriptor");
            }
            return address;
        } catch (InvalidProtocolBufferException ex) {
            throw new IllegalArgumentException("Malformed shuffle allocation descriptor", ex);
        }
    }
}
