package org.pangrova.coordinator.query.execution.dispatch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.pangrova.connector.spi.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.spi.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.spi.metadata.table.SchemaTableName;

/** A real, bounded round-trip codec for the in-memory DML connector fixtures. */
final class FixtureOutputHandleCodec implements ConnectorOutputHandleCodec {

    private static final int VERSION = 1;

    private final String typeUrl;
    private final SchemaTableName target;
    private final Set<String> modes;
    private final List<String> columns;
    private final Function<String, ConnectorOutputTableHandle> reconstruct;

    FixtureOutputHandleCodec(String typeUrl, SchemaTableName target, Set<String> modes,
                             List<String> columns,
                             Function<String, ConnectorOutputTableHandle> reconstruct) {
        this.typeUrl = Objects.requireNonNull(typeUrl);
        this.target = Objects.requireNonNull(target);
        this.modes = Set.copyOf(modes);
        this.columns = List.copyOf(columns);
        this.reconstruct = Objects.requireNonNull(reconstruct);
    }

    @Override
    public String typeUrl() {
        return typeUrl;
    }

    @Override
    public int codecVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(ConnectorOutputTableHandle handle) {
        validate(handle);
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeUTF(target.schema());
            out.writeUTF(target.table());
            out.writeUTF(handle.writeMode());
            out.flush();
            return bytes.toByteArray();
        } catch (IOException failure) {
            throw new IllegalStateException("Fixture output handle encoding failed", failure);
        }
    }

    @Override
    public ConnectorOutputTableHandle deserialize(int version, byte[] payload) {
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported fixture output handle version: " + version);
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            SchemaTableName decodedTarget = new SchemaTableName(in.readUTF(), in.readUTF());
            String mode = in.readUTF();
            if (in.available() != 0 || !target.equals(decodedTarget) || !modes.contains(mode)) {
                throw new IllegalArgumentException("Fixture output handle binding mismatch");
            }
            ConnectorOutputTableHandle handle = reconstruct.apply(mode);
            validate(handle);
            return handle;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Malformed fixture output handle", failure);
        }
    }

    @Override
    public List<String> writeColumnNames(ConnectorOutputTableHandle handle) {
        validate(handle);
        return columns;
    }

    private void validate(ConnectorOutputTableHandle handle) {
        if (handle == null || !handle.targetTable().filter(target::equals).isPresent()
                || !modes.contains(handle.writeMode())) {
            throw new IllegalArgumentException("Fixture output handle binding mismatch");
        }
    }
}
