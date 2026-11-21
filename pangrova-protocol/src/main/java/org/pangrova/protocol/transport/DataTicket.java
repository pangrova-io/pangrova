package org.pangrova.protocol.transport;

import org.pangrova.protocol.enums.DataTicketType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * Immutable ticket identifying a data partition in the data transport plane.
 * Uses DataOutputStream/DataInputStream for lightweight serialization without Protobuf.
 */
public record DataTicket(String queryId, String taskId, int outputPartition, DataTicketType type) {

    public DataTicket {
        Objects.requireNonNull(queryId, "queryId must not be null");
        Objects.requireNonNull(taskId, "taskId must not be null");
        Objects.requireNonNull(type, "type must not be null");
    }

    /**
     * Serializes this ticket to a portable byte array.
     */
    public byte[] serialize() {
        try (var baos = new ByteArrayOutputStream(); var out = new DataOutputStream(baos)) {
            out.writeUTF(queryId);
            out.writeUTF(taskId);
            out.writeInt(outputPartition);
            out.writeInt(type.ordinal());
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize DataTicket", e);
        }
    }

    /**
     * Deserializes a DataTicket from a byte array produced by {@link #serialize()}.
     */
    public static DataTicket deserialize(byte[] data) {
        Objects.requireNonNull(data, "data must not be null");
        try (var in = new DataInputStream(new ByteArrayInputStream(data))) {
            String queryId = in.readUTF();
            String taskId = in.readUTF();
            int partition = in.readInt();
            DataTicketType type = DataTicketType.values()[in.readInt()];
            return new DataTicket(queryId, taskId, partition, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize DataTicket", e);
        }
    }
}
