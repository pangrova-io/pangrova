package org.pangrova.coordinator.query.execution;

import com.google.protobuf.ByteString;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.coordinator.metadata.CatalogExecutionBindings;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ConnectorWriteBinding;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.NativePhysicalWriteAllocation;

import org.pangrova.protocol.CatalogDeploymentFingerprint;
import org.pangrova.protocol.PhysicalFileWriteBindings;

/** Builds the immutable connector write contract dispatched to Workers. */
public final class ConnectorWriteBindingFactory {

    private ConnectorWriteBindingFactory() {}

    public static WriteOutputBinding create(
            CatalogHandle catalogHandle,
            ConnectorOutputTableHandle outputHandle) {
        if (!(catalogHandle instanceof ConnectorCatalogHandle connectorCatalog)) {
            throw new IllegalArgumentException(
                    "Distributed connector write requires a connector catalog");
        }
        CatalogDescriptor descriptor = connectorCatalog.getDescriptor();
        var serialized = serialize(connectorCatalog.getOutputHandleCodec(), outputHandle);
        var allocation = outputHandle.nativeWriteAllocation();
        var physical = outputHandle.nativePhysicalWriteAllocation();
        if (allocation.isPresent() && physical.isPresent()) {
            throw new IllegalArgumentException("One output handle cannot own both Dataset and physical sinks");
        }
        if (physical.isPresent()) return physical(catalogBinding(descriptor), serialized, physical.orElseThrow());
        if (allocation.isPresent()) return lance(catalogBinding(descriptor), serialized, allocation.orElseThrow());
        return WriteOutputBinding.newBuilder().setConnector(ConnectorWriteBinding.newBuilder()
                .setCatalog(catalogBinding(descriptor)).setOutputHandle(serialized)).build();
    }

    private static SerializedConnectorOutputHandle serialize(
            ConnectorOutputHandleCodec codec, ConnectorOutputTableHandle outputHandle) {
        byte[] payload = codec.serialize(outputHandle);
        return SerializedConnectorOutputHandle.newBuilder()
                .setTypeUrl(codec.typeUrl())
                .setCodecVersion(codec.codecVersion())
                .setPayload(ByteString.copyFrom(payload))
                .setPayloadChecksum(checksum(payload))
                .addAllWriteColumnNames(codec.writeColumnNames(outputHandle)).build();
    }

    private static WriteOutputBinding lance(CatalogExecutionBinding catalog,
            SerializedConnectorOutputHandle serialized, NativeWriteAllocation nativeOutput) {
            if (!nativeOutput.writeColumnNames().equals(serialized.getWriteColumnNamesList())) {
                throw new IllegalArgumentException("Native allocation columns differ from connector handle");
            }
            return WriteOutputBinding.newBuilder().setLance(LanceWriteBinding.newBuilder()
                    .setCatalog(catalog).setOutputHandle(serialized)
                    .setDatasetUri(nativeOutput.datasetUri())
                    .setOperation(switch (nativeOutput.operation()) {
                        case CREATE -> LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE;
                        case APPEND -> LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND;
                        case OVERWRITE -> LanceWriteOperation.LANCE_WRITE_OPERATION_OVERWRITE;
                    })
                    .setExpectedReadVersion(nativeOutput.readVersion())
                    .setSchemaFenceSha256(Checksum.newBuilder()
                            .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                            .setDigest(ByteString.copyFrom(nativeOutput.schemaFenceSha256())))
                    .setWriteId(nativeOutput.writeId()).setGeneration(nativeOutput.generation())
                    .setTransactionStagingPrefix(nativeOutput.stagingPrefix())
                    .setAllocationStagingPrefix(nativeOutput.stagingPrefix())
                    .addAllWriteColumnNames(nativeOutput.writeColumnNames())).build();
    }

    private static WriteOutputBinding physical(CatalogExecutionBinding catalog,
            SerializedConnectorOutputHandle serialized, NativePhysicalWriteAllocation allocation) {
        var binding = PhysicalFileWriteBindings.template(allocation);
        if (!binding.getColumnsList().stream().map(column -> column.getName()).toList()
                .equals(serialized.getWriteColumnNamesList())) {
            throw new IllegalArgumentException("Physical allocation columns differ from connector handle");
        }
        return WriteOutputBinding.newBuilder().setPhysical(binding.toBuilder()
                .setCatalog(catalog).setOutputHandle(serialized)).build();
    }

    private static CatalogExecutionBinding catalogBinding(CatalogDescriptor descriptor) {
        return CatalogExecutionBindings.from(descriptor);
    }

    private static Checksum checksum(byte[] value) {
        return CatalogDeploymentFingerprint.sha256(value);
    }
}
