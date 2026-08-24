package org.pangrova.protocol.validation;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/**
 * Utility that combines proto deserialization with field-level validation.
 *
 * <p>Wraps {@link ProtoValidator} into a single parse-and-validate call,
 * ensuring that every proto message deserialized through this codec is
 * checked against field constraints (required fields, value ranges, etc.).</p>
 *
 * <p>Usage at deserialization points:
 * <pre>{@code
 *   SplitInfo split = ValidatingProtoCodec.parseAndValidate(
 *       SplitInfo.parser(), bytes);
 * }</pre>
 *
 * <p>This class addresses finding F-PROTO-02: ProtoValidator was previously
 * dead code never invoked at any deserialization point.</p>
 */
public final class ValidatingProtoCodec {

    private ValidatingProtoCodec() {}

    /**
     * Parses a proto message from bytes and validates field constraints.
     *
     * @param parser the proto parser for the target message type
     * @param bytes  the serialized proto bytes
     * @param <T>    the proto message type
     * @return the parsed and validated message
     * @throws InvalidProtocolBufferException if parsing fails
     * @throws ProtoValidationException       if field validation fails
     */
    public static <T extends Message> T parseAndValidate(Parser<T> parser, byte[] bytes)
            throws InvalidProtocolBufferException {
        T message = parser.parseFrom(bytes);
        ProtoValidator.validate(message);
        return message;
    }

    /**
     * Validates an already-parsed proto message.
     * Convenience method for cases where parsing is done externally
     * (e.g., via generated builder) but validation is still desired.
     *
     * @param message the proto message to validate
     * @param <T>     the proto message type
     * @return the same message if valid
     * @throws ProtoValidationException if field validation fails
     */
    public static <T extends Message> T validate(T message) {
        ProtoValidator.validate(message);
        return message;
    }
}
