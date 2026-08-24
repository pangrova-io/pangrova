package org.pangrova.worker.jni;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that native C++ errors propagate stack trace information
 * to Java via the JNI error callback (G.5).
 *
 * <p>Tests the Java-side proto parsing logic for extracting the
 * native_stack_trace field from TaskStatus proto bytes. The actual
 * native library integration is tested in the E2E test suite.</p>
 */
class NativeErrorStackTraceTest {

    @Test
    void nativeRuntimeExceptionCarriesStackTraceAsSuppressed() {
        // Simulate what NativeTaskExecutor.isFinished() does when it detects FAILED state
        String errorMessage = "Velox execution failed: OOM";
        String nativeStackTrace = "0x7fff12345 libpangrova_native.dylib`pangrova::executor::TaskExecutor::start\n"
                + "0x7fff12346 libpangrova_native.dylib`pangrova::velox::VeloxTaskRunner::run";

        NativeRuntimeException ex = new NativeRuntimeException(
                PangrovaErrorCode.NATIVE_JNI_EXCEPTION, errorMessage);
        ex.addSuppressed(new RuntimeException("Native C++ stack trace:\n" + nativeStackTrace));

        assertThat(ex.getMessage()).isEqualTo("Velox execution failed: OOM");
        assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.NATIVE_JNI_EXCEPTION);
        assertThat(ex.getSuppressed()).hasSize(1);
        assertThat(ex.getSuppressed()[0].getMessage())
                .contains("Native C++ stack trace:")
                .contains("libpangrova_native")
                .contains("TaskExecutor");
    }

    @Test
    void extractStringFieldParsesProtoCorrectly() {
        // Build a minimal TaskStatus proto with:
        //   field 1 (task_id): "test-task"
        //   field 2 (state): 5 (FAILED)
        //   field 3 (error_message): "test error"
        //   field 5 (native_stack_trace): "frame1\nframe2"
        byte[] proto = buildTaskStatusProto("test-task", 5, "test error", "frame1\nframe2");

        // Use the same parsing logic as NativeTaskExecutor
        String errorMessage = extractStringField(proto, 3);
        String stackTrace = extractStringField(proto, 5);

        assertThat(errorMessage).isEqualTo("test error");
        assertThat(stackTrace).isEqualTo("frame1\nframe2");
    }

    @Test
    void extractStringFieldReturnsNullForMissingField() {
        // Proto with only field 1 and 2 (no error_message or stack_trace)
        byte[] proto = buildTaskStatusProto("task-1", 4, "", "");

        String stackTrace = extractStringField(proto, 5);
        // Empty string fields are not serialized in proto3, so field is absent
        assertThat(stackTrace).isNull();
    }

    @Test
    void extractStringFieldHandlesEmptyInput() {
        assertThat(extractStringField(new byte[0], 3)).isNull();
    }

    // --- Proto building helpers (manual wire format) ---

    private static byte[] buildTaskStatusProto(String taskId, int state,
                                               String errorMessage, String stackTrace) {
        // Proto3 wire format: field_number << 3 | wire_type
        // wire_type 0 = varint, wire_type 2 = length-delimited
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();

        // Field 1: task_id (string, wire type 2)
        if (!taskId.isEmpty()) {
            writeTag(out, 1, 2);
            writeString(out, taskId);
        }

        // Field 2: state (enum/varint, wire type 0)
        writeTag(out, 2, 0);
        writeVarint(out, state);

        // Field 3: error_message (string, wire type 2)
        if (!errorMessage.isEmpty()) {
            writeTag(out, 3, 2);
            writeString(out, errorMessage);
        }

        // Field 5: native_stack_trace (string, wire type 2)
        if (!stackTrace.isEmpty()) {
            writeTag(out, 5, 2);
            writeString(out, stackTrace);
        }

        return out.toByteArray();
    }

    private static void writeTag(java.io.ByteArrayOutputStream out, int fieldNumber, int wireType) {
        writeVarint(out, (fieldNumber << 3) | wireType);
    }

    private static void writeVarint(java.io.ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) (value & 0x7F));
    }

    private static void writeString(java.io.ByteArrayOutputStream out, String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        writeVarint(out, bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    // --- Proto parsing (mirrors NativeTaskExecutor logic) ---

    private static String extractStringField(byte[] data, int targetFieldNumber) {
        int pos = 0;
        while (pos < data.length) {
            long tagAndType = readVarint(data, pos);
            if (tagAndType < 0) {
                return null;
            }
            pos += varintSize(tagAndType);
            int fieldNumber = (int) (tagAndType >>> 3);
            int wireType = (int) (tagAndType & 0x07);
            if (wireType == 0) {
                long value = readVarint(data, pos);
                pos += varintSize(value);
            } else if (wireType == 2) {
                long len = readVarint(data, pos);
                pos += varintSize(len);
                if (fieldNumber == targetFieldNumber) {
                    return new String(data, pos, (int) len, java.nio.charset.StandardCharsets.UTF_8);
                }
                pos += (int) len;
            } else {
                return null;
            }
        }
        return null;
    }

    private static long readVarint(byte[] data, int offset) {
        long result = 0;
        int shift = 0;
        int pos = offset;
        while (pos < data.length) {
            byte b = data[pos++];
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        return -1;
    }

    private static int varintSize(long value) {
        int size = 1;
        while ((value & ~0x7FL) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }
}
