package org.pangrova.connector.lance;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import java.io.IOException;
import java.util.List;

/** Strict bounded deterministic encoding for exact Lance output allocations. */
public final class LanceOutputHandleCodec implements ConnectorOutputHandleCodec {
    static final int MAX_PAYLOAD = 1024 * 1024;
    static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    @Override public String typeUrl() { return "pangrova.io/connector/lance/output-handle"; }
    @Override public int codecVersion() { return 1; }
    @Override public byte[] serialize(ConnectorOutputTableHandle handle) {
        if (!(handle instanceof LanceOutputHandle lance)) {
            throw new IllegalArgumentException("Expected Lance output handle");
        }
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(lance);
            if (bytes.length > MAX_PAYLOAD) {
                throw new IllegalArgumentException("Lance output handle exceeds metadata limit");
            }
            return bytes;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Cannot encode Lance output handle", failure);
        }
    }
    @Override public ConnectorOutputTableHandle deserialize(int version, byte[] payload) {
        if (version != 1 || payload == null || payload.length == 0 || payload.length > MAX_PAYLOAD) {
            throw new IllegalArgumentException("Invalid Lance output handle version or size");
        }
        try {
            LanceOutputHandle handle = MAPPER.readValue(payload, LanceOutputHandle.class);
            if (!java.util.Arrays.equals(serialize(handle), payload)) {
                throw new IllegalArgumentException("Lance output handle encoding is not canonical");
            }
            return handle;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Malformed Lance output handle", failure);
        }
    }
    @Override public List<String> writeColumnNames(ConnectorOutputTableHandle handle) {
        if (!(handle instanceof LanceOutputHandle lance)) {
            throw new IllegalArgumentException("Expected Lance output handle");
        }
        return lance.allocation().writeColumnNames();
    }
}
