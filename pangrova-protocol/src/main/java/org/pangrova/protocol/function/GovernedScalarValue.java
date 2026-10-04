package org.pangrova.protocol.function;

import com.google.protobuf.ByteString;

/** Immutable closed control-value union; not another SQL type or feature protocol. */
public sealed interface GovernedScalarValue {
    record Text(String value) implements GovernedScalarValue {
        public Text {
            if (value == null || value.isBlank()) throw new IllegalArgumentException("Binding text must be nonblank");
        }
    }

    record Signature(ByteString value) implements GovernedScalarValue {
        public Signature {
            if (value == null || value.size() != 32) {
                throw new IllegalArgumentException("Binding SHA-256 must be 32 bytes");
            }
        }
    }

    record Unsigned(long value) implements GovernedScalarValue {
        public Unsigned {
            if (value < 0) throw new IllegalArgumentException("Binding exceeds Java unsigned accounting domain");
        }
    }

    record Flag(boolean value) implements GovernedScalarValue {}

    record Floating(double value) implements GovernedScalarValue {
        public Floating {
            if (!Double.isFinite(value)) throw new IllegalArgumentException("Binding floating value must be finite");
        }
    }
}
