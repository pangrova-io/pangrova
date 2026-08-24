package org.pangrova.worker.jni;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Loads native engine capabilities by sending {@code GET_CAPABILITIES} via
 * the unified command channel.
 *
 * <p>Called once during worker startup. The proto response is parsed using
 * manual wire-format decoding (same pattern as {@link NativeCommandBuilder})
 * to avoid a compile dependency on native proto generated classes.</p>
 *
 * <p>The loaded capability sets are delivered to the caller via the
 * {@link CapabilityConsumer} callback, keeping this class free of any
 * dependency on {@code pangrova-planner}.</p>
 */
public final class NativeCapabilityLoader {

    private static final Logger LOG =
            Logger.getLogger(NativeCapabilityLoader.class.getName());

    private NativeCapabilityLoader() {}

    /**
     * Callback that receives the parsed capability sets.
     * Typically wired to {@code NativeCapabilityRegistry::update}.
     */
    @FunctionalInterface
    public interface CapabilityConsumer {
        void accept(Set<String> relTypes, Set<String> exprTypes,
                    Set<String> functions);
    }

    /**
     * Sends {@code GET_CAPABILITIES} command via {@link NativeRuntime},
     * parses the {@code NativeCapabilityResponse} proto, converts repeated
     * string fields to {@code Set<String>}, and delivers them to the
     * provided consumer.
     *
     * @param consumer callback to receive the parsed capability sets
     *                 (typically {@code NativeCapabilityRegistry::update})
     * @throws IllegalStateException if the native command fails or the
     *         response cannot be parsed
     */
    public static void loadCapabilities(CapabilityConsumer consumer) {
        byte[] commandBytes = NativeCommandBuilder.buildGetCapabilities();
        byte[] responseBytes = NativeRuntime.nativeCommand(0, commandBytes);

        if (responseBytes == null || responseBytes.length == 0) {
            throw new IllegalStateException(
                    "GET_CAPABILITIES returned empty response");
        }

        byte[] payload = NativeCommandBuilder.extractResultPayload(responseBytes);
        if (payload == null || payload.length == 0) {
            throw new IllegalStateException(
                    "GET_CAPABILITIES response has no result_payload");
        }

        ParsedCapabilities parsed = parseCapabilityResponse(payload);

        LOG.info(String.format(
                "event=native_capabilities_loaded relTypes=%d exprTypes=%d "
                        + "functions=%d engineVersion=%s",
                parsed.relTypes.size(), parsed.exprTypes.size(),
                parsed.functions.size(), parsed.engineVersion));

        consumer.accept(parsed.relTypes, parsed.exprTypes, parsed.functions);
    }

    // --- Proto parsing -------------------------------------------------------

    /**
     * Parses a serialized {@code NativeCapabilityResponse} proto into
     * plain Java sets. Wire format:
     * <ul>
     *   <li>Field 1 (repeated string): supported_rel_types</li>
     *   <li>Field 2 (repeated string): supported_expr_types</li>
     *   <li>Field 3 (repeated string): supported_functions</li>
     *   <li>Field 4 (string): engine_version</li>
     * </ul>
     */
    static ParsedCapabilities parseCapabilityResponse(byte[] payload) {
        Set<String> relTypes = new LinkedHashSet<>();
        Set<String> exprTypes = new LinkedHashSet<>();
        Set<String> functions = new LinkedHashSet<>();
        String[] engineVersion = {""};
        Set<?>[] targets = {relTypes, exprTypes, functions};

        int[] cursor = {0};
        while (cursor[0] < payload.length) {
            int advanced = parseNextField(payload, cursor[0], targets, engineVersion);
            if (advanced <= 0) {
                break;
            }
            cursor[0] += advanced;
        }
        return new ParsedCapabilities(relTypes, exprTypes, functions, engineVersion[0]);
    }

    private static int parseNextField(byte[] payload, int pos,
                                      Set<?>[] targets, String[] engineVersion) {
        long tagAndType = readVarint(payload, pos);
        if (tagAndType < 0) {
            return -1;
        }
        int consumed = varintSize(tagAndType);
        int fieldNumber = (int) (tagAndType >>> 3);
        int wireType = (int) (tagAndType & 0x07);

        return switch (wireType) {
            case 2 -> parseLengthDelimited(payload, pos, consumed, fieldNumber, targets, engineVersion);
            case 0 -> skipVarintField(payload, pos, consumed);
            default -> -1;
        };
    }

    @SuppressWarnings("unchecked")
    private static int parseLengthDelimited(byte[] payload, int pos, int consumed,
                                            int fieldNumber, Set<?>[] targets,
                                            String[] engineVersion) {
        long len = readVarint(payload, pos + consumed);
        consumed += varintSize(len);
        String value = new String(payload, pos + consumed, (int) len, StandardCharsets.UTF_8);
        consumed += (int) len;
        if (fieldNumber >= 1 && fieldNumber <= 3) {
            ((Set<String>) targets[fieldNumber - 1]).add(value);
        } else if (fieldNumber == 4) {
            engineVersion[0] = value;
        }
        return consumed;
    }

    private static int skipVarintField(byte[] payload, int pos, int consumed) {
        long val = readVarint(payload, pos + consumed);
        return val < 0 ? -1 : consumed + varintSize(val);
    }

    /** Masks the value bits of a single varint-encoded byte. */
    private static final int VARINT_VALUE_MASK = 0x7F;

    /** Masks the value bits of a single varint-encoded byte (long form). */
    private static final long VARINT_VALUE_MASK_LONG = 0x7FL;

    private static long readVarint(byte[] data, int offset) {
        long result = 0;
        int shift = 0;
        int pos = offset;
        while (pos < data.length) {
            byte b = data[pos++];
            result |= (long) (b & VARINT_VALUE_MASK) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        return -1;
    }

    private static int varintSize(long value) {
        int size = 1;
        while ((value & ~VARINT_VALUE_MASK_LONG) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }

    /** Parsed result holder (package-private for testing). */
    record ParsedCapabilities(
            Set<String> relTypes,
            Set<String> exprTypes,
            Set<String> functions,
            String engineVersion) {}

}
