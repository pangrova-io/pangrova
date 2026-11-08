package org.pangrova.planner.core.substrait;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.substrait.proto.Plan;
import io.substrait.proto.ReadRel;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Extracts exact scan schemas from a server-compiled Substrait plan.
 * This structural operation does not grant or verify authorization.
 */
public final class CompiledScanSchemas {

    private CompiledScanSchemas() {}

    /** Extracts canonical scan schemas from a serialized Substrait plan. */
    public static Map<String, byte[]> extract(byte[] bytes) {
        try {
            Map<String, byte[]> result = new HashMap<>();
            visit(Plan.parseFrom(bytes), result);
            return result;
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("Server-compiled relation is not canonical Substrait", failure);
        }
    }

    private static void visit(Message message, Map<String, byte[]> result) {
        recordSchema(message, result);
        for (Descriptors.FieldDescriptor field : message.getAllFields().keySet()) {
            if (field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            Object value = message.getField(field);
            if (field.isRepeated()) {
                for (Object child : (List<?>) value) {
                    visit((Message) child, result);
                }
            } else {
                visit((Message) value, result);
            }
        }
    }

    private static void recordSchema(Message message, Map<String, byte[]> result) {
        if (message instanceof ReadRel read && read.hasNamedTable()) {
            String table = String.join(".", read.getNamedTable().getNamesList());
            byte[] schema = read.getBaseSchema().toByteArray();
            byte[] previous = result.putIfAbsent(table, schema);
            if (previous != null && !Arrays.equals(previous, schema)) {
                throw new IllegalArgumentException("Repeated source has inconsistent scan schemas: " + table);
            }
        }
    }
}
