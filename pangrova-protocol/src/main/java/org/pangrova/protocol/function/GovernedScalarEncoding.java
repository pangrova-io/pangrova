package org.pangrova.protocol.function;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.Base64;

/** Interpreter for the sole declared encoding, shared by projection and callback comparison. */
final class GovernedScalarEncoding {
    private GovernedScalarEncoding() {}

    static GovernedScalarValue field(FieldDescriptor field, Object value, String encoding) {
        return switch (encoding) {
            case "STRING" -> {
                require(field.getType() == FieldDescriptor.Type.STRING);
                yield new GovernedScalarValue.Text((String) value);
            }
            case "BASE64" -> {
                require(field.getType() == FieldDescriptor.Type.BYTES);
                yield new GovernedScalarValue.Signature((ByteString) value);
            }
            case "UNSIGNED_DECIMAL" -> {
                require(field.getType() == FieldDescriptor.Type.UINT32 || field.getType() == FieldDescriptor.Type.UINT64);
                yield new GovernedScalarValue.Unsigned(((Number) value).longValue());
            }
            case "BOOLEAN" -> {
                require(field.getType() == FieldDescriptor.Type.BOOL);
                yield new GovernedScalarValue.Flag((Boolean) value);
            }
            case "EXACT_FLOAT" -> {
                require(field.getType() == FieldDescriptor.Type.DOUBLE);
                yield new GovernedScalarValue.Floating((Double) value);
            }
            default -> throw new IllegalArgumentException("Unknown governed encoding: " + encoding);
        };
    }

    static GovernedScalarValue decode(String value, String encoding) {
        if (value == null) throw new IllegalArgumentException("Binding value must not be NULL");
        return switch (encoding) {
            case "STRING" -> new GovernedScalarValue.Text(value);
            case "BASE64" -> signature(value);
            case "UNSIGNED_DECIMAL" -> unsigned(value);
            case "BOOLEAN" -> {
                if (!value.equals("true") && !value.equals("false")) throw new IllegalArgumentException("Invalid binding boolean");
                yield new GovernedScalarValue.Flag(Boolean.parseBoolean(value));
            }
            case "EXACT_FLOAT" -> {
                if (!value.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?")) {
                    throw new IllegalArgumentException("Invalid decimal floating binding");
                }
                yield new GovernedScalarValue.Floating(Double.parseDouble(value));
            }
            default -> throw new IllegalArgumentException("Unknown governed encoding: " + encoding);
        };
    }

    private static GovernedScalarValue signature(String encoded) {
        var bytes = ByteString.copyFrom(Base64.getDecoder().decode(encoded));
        if (!Base64.getEncoder().encodeToString(bytes.toByteArray()).equals(encoded)) {
            throw new IllegalArgumentException("Binding signature must be canonical Base64");
        }
        return new GovernedScalarValue.Signature(bytes);
    }

    private static GovernedScalarValue unsigned(String value) {
        if (!value.matches("0|[1-9][0-9]*")) throw new IllegalArgumentException("Invalid unsigned binding decimal");
        return new GovernedScalarValue.Unsigned(Long.parseLong(value));
    }

    private static void require(boolean valid) {
        if (!valid) throw new IllegalArgumentException("Binding field differs from its declared encoding");
    }
}
