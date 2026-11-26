package org.pangrova.protocol.descriptor;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.protocol.v2.ProtocolVersion;

/** Exact wire identity shared by startup handshakes and execution admission. */
@Internal
public final class CanonicalProtocolIdentity {
    public static final int PROTOCOL_MAJOR = 2;
    public static final int PROTOCOL_MINOR = 0;
    public static final String ROLLOUT_EPOCH = "ai-native-function-resource-r3";

    private static final ProtocolVersion CURRENT = ProtocolVersion.newBuilder()
            .setMajor(PROTOCOL_MAJOR).setMinor(PROTOCOL_MINOR)
            .setDescriptorSetSha256(CanonicalProtocolDescriptors.sha256Digest())
            .setRolloutEpoch(ROLLOUT_EPOCH).build();

    private CanonicalProtocolIdentity() {}

    /** Returns the current version, descriptor digest and rollout identity. */
    public static ProtocolVersion current() {
        return CURRENT;
    }

    /** Rejects a different wire contract or undeclared protocol identity fields. */
    public static void validate(ProtocolVersion protocol) {
        if (!CURRENT.equals(protocol)) {
            throw new IllegalArgumentException("TaskRequest protocol identity does not match canonical v2");
        }
    }
}
