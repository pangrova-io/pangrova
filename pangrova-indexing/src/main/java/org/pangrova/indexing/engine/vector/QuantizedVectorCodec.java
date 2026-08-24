/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.indexing.engine.vector;

import org.pangrova.spi.index.vector.QuantizationType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Codec for quantizing fp32 vectors to reduced-precision representations
 * (fp16, int8, binary) and decoding them back.
 *
 * <p>Used during index build to compress vectors before storage, and during
 * search to decode stored vectors for distance computation or reranking.</p>
 *
 * <p>The codec is stateless and thread-safe.</p>
 */
final class QuantizedVectorCodec {

    /** Maximum quantized level for 8-bit scalar quantization. */
    private static final int INT8_MAX_VALUE = 255;
    /** Mask to widen a signed byte to unsigned. */
    private static final int BYTE_MASK = 0xFF;

    // IEEE-754 single precision layout
    private static final int FLOAT_SIGN_SHIFT = 16;
    private static final int FLOAT_EXPONENT_SHIFT = 23;
    private static final int FLOAT_EXPONENT_MASK = 0xFF;
    private static final int FLOAT_EXPONENT_BIAS = 127;
    private static final int FLOAT_MANTISSA_MASK = 0x7FFFFF;
    private static final int FLOAT_INFINITY_BITS = 0x7F800000;

    // IEEE-754 half precision layout
    private static final int HALF_SIGN_MASK = 0x8000;
    private static final int HALF_MANTISSA_SHIFT = 10;
    private static final int HALF_EXPONENT_MASK = 0x1F;
    private static final int HALF_EXPONENT_MAX = 31;
    private static final int HALF_EXPONENT_BITS = 0x7C00;
    private static final int HALF_EXPONENT_BIAS = 15;
    private static final int HALF_MANTISSA_MASK = 0x3FF;
    /** Mantissa width difference between fp32 and fp16. */
    private static final int MANTISSA_SHIFT_DIFF = 13;

    private QuantizedVectorCodec() {
    }

    static byte[] encode(float[] vector, QuantizationType type) {
        return switch (type) {
            case FP32 -> encodeFp32(vector);
            case FP16 -> encodeFp16(vector);
            case INT8 -> encodeInt8(vector);
            case BINARY -> encodeBinary(vector);
        };
    }

    static float[] decode(byte[] encoded, int dimension, QuantizationType type) {
        return switch (type) {
            case FP32 -> decodeFp32(encoded, dimension);
            case FP16 -> decodeFp16(encoded, dimension);
            case INT8 -> decodeInt8(encoded, dimension);
            case BINARY -> decodeBinary(encoded, dimension);
        };
    }

    static int encodedSizeBytes(int dimension, QuantizationType type) {
        return switch (type) {
            case FP32 -> dimension * 4;
            case FP16 -> dimension * 2;
            case INT8 -> dimension + 8;
            case BINARY -> (dimension + 7) / 8;
        };
    }

    private static byte[] encodeFp32(float[] vector) {
        ByteBuffer buf = ByteBuffer.allocate(vector.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (float v : vector) {
            buf.putFloat(v);
        }
        return buf.array();
    }

    private static float[] decodeFp32(byte[] encoded, int dimension) {
        ByteBuffer buf = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN);
        float[] result = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            result[i] = buf.getFloat();
        }
        return result;
    }

    private static byte[] encodeFp16(float[] vector) {
        ByteBuffer buf = ByteBuffer.allocate(vector.length * 2).order(ByteOrder.LITTLE_ENDIAN);
        for (float v : vector) {
            buf.putShort(floatToHalf(v));
        }
        return buf.array();
    }

    private static float[] decodeFp16(byte[] encoded, int dimension) {
        ByteBuffer buf = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN);
        float[] result = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            result[i] = halfToFloat(buf.getShort());
        }
        return result;
    }

    private static byte[] encodeInt8(float[] vector) {
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        for (float v : vector) {
            if (v < min) min = v;
            if (v > max) max = v;
        }
        float range = max - min;
        float scale = range > 0 ? range / INT8_MAX_VALUE : 1.0f;

        ByteBuffer buf = ByteBuffer.allocate(8 + vector.length).order(ByteOrder.LITTLE_ENDIAN);
        buf.putFloat(min);
        buf.putFloat(scale);
        for (float v : vector) {
            int quantized = Math.round((v - min) / scale);
            quantized = Math.max(0, Math.min(INT8_MAX_VALUE, quantized));
            buf.put((byte) quantized);
        }
        return buf.array();
    }

    private static float[] decodeInt8(byte[] encoded, int dimension) {
        ByteBuffer buf = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN);
        float min = buf.getFloat();
        float scale = buf.getFloat();
        float[] result = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            result[i] = min + (buf.get() & BYTE_MASK) * scale;
        }
        return result;
    }

    private static byte[] encodeBinary(float[] vector) {
        float mean = 0;
        for (float v : vector) {
            mean += v;
        }
        mean /= vector.length;

        int byteCount = (vector.length + 7) / 8;
        byte[] result = new byte[byteCount];
        for (int i = 0; i < vector.length; i++) {
            if (vector[i] >= mean) {
                result[i >> 3] |= (byte) (1 << (i & 7));
            }
        }
        return result;
    }

    private static float[] decodeBinary(byte[] encoded, int dimension) {
        float[] result = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            boolean bit = (encoded[i >> 3] & (1 << (i & 7))) != 0;
            result[i] = bit ? 1.0f : -1.0f;
        }
        return result;
    }

    static short floatToHalf(float f) {
        int bits = Float.floatToRawIntBits(f);
        int sign = (bits >> FLOAT_SIGN_SHIFT) & HALF_SIGN_MASK;
        int exponent = ((bits >> FLOAT_EXPONENT_SHIFT) & FLOAT_EXPONENT_MASK)
                - FLOAT_EXPONENT_BIAS + HALF_EXPONENT_BIAS;
        int mantissa = bits & FLOAT_MANTISSA_MASK;
        if (exponent <= 0) {
            return (short) sign;
        }
        if (exponent >= HALF_EXPONENT_MAX) {
            return (short) (sign | HALF_EXPONENT_BITS);
        }
        return (short) (sign | (exponent << HALF_MANTISSA_SHIFT) | (mantissa >> MANTISSA_SHIFT_DIFF));
    }

    static float halfToFloat(short h) {
        int sign = (h & HALF_SIGN_MASK) << FLOAT_SIGN_SHIFT;
        int exponent = (h >> HALF_MANTISSA_SHIFT) & HALF_EXPONENT_MASK;
        int mantissa = h & HALF_MANTISSA_MASK;
        if (exponent == 0) {
            return Float.intBitsToFloat(sign);
        }
        if (exponent == HALF_EXPONENT_MAX) {
            return Float.intBitsToFloat(sign | FLOAT_INFINITY_BITS
                    | (mantissa << MANTISSA_SHIFT_DIFF));
        }
        return Float.intBitsToFloat(sign
                | ((exponent + FLOAT_EXPONENT_BIAS - HALF_EXPONENT_BIAS) << FLOAT_EXPONENT_SHIFT)
                | (mantissa << MANTISSA_SHIFT_DIFF));
    }
}
