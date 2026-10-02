package org.pangrova.worker.jni;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.CapabilityInventoryCodec;
import org.pangrova.protocol.v2.CapabilityInventory;
import org.pangrova.protocol.v2.CapabilityResponse;
import org.pangrova.protocol.v2.FunctionAvailability;
import org.pangrova.protocol.v2.FunctionCatalogManifest;

import java.util.List;
import java.util.Set;

/** Loads and fail-closed validates the sole canonical v2 Native capability inventory. */
public final class NativeCapabilityLoader {

    private NativeCapabilityLoader() {}

    /** Returns the complete validated inventory for lossless Worker registration. */
    public static ParsedCapabilities loadInventory() {
        byte[] responseBytes = NativeRuntime.nativeCommand(
                0, NativeCommandBuilder.buildGetCapabilities());
        byte[] payload = NativeCommandBuilder.extractResultPayload(responseBytes);
        if (payload == null || payload.length == 0) {
            throw new IllegalStateException("GET_CAPABILITIES response has no v2 payload");
        }
        return parseCapabilityResponse(payload);
    }

    /** Parses CapabilityResponse and validates identity, limits, version and descriptor binding. */
    static ParsedCapabilities parseCapabilityResponse(byte[] payload) {
        final CapabilityResponse response;
        try {
            response = CapabilityResponse.parseFrom(payload);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Malformed native v2 capability response", failure);
        }
        if (!response.hasInventory()) {
            throw new IllegalStateException("Native v2 capability response has no inventory");
        }

        CapabilityInventory inventory = response.getInventory();
        try {
            CapabilityInventoryCodec.validate(inventory);
        } catch (IllegalArgumentException failure) {
            throw new IllegalStateException(failure.getMessage(), failure);
        }
        return new ParsedCapabilities(
                Set.copyOf(inventory.getSupportedSubstraitRelTypesList()),
                Set.copyOf(inventory.getSupportedSubstraitExpressionTypesList()),
                List.copyOf(inventory.getFunctionsList()),
                inventory.getFunctionCatalog(),
                inventory.getNativeEngineVersion(),
                inventory);
    }

    public record ParsedCapabilities(
            Set<String> relTypes,
            Set<String> exprTypes,
            List<FunctionAvailability> functions,
            FunctionCatalogManifest functionCatalog,
            String engineVersion,
            CapabilityInventory inventory) {}
}
