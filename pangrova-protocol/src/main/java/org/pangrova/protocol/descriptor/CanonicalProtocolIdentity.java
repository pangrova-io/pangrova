package org.pangrova.protocol.descriptor;

import java.util.Arrays;

import com.google.protobuf.ByteString;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.protocol.v2.ProtocolVersion;

/** Exact wire identity shared by startup handshakes and execution admission. */
@Internal
public final class CanonicalProtocolIdentity {
    public static final int PROTOCOL_MAJOR = 2;
    public static final int PROTOCOL_MINOR = 0;
    public static final String ROLLOUT_EPOCH = "ai-native-function-resource-r3";

    private CanonicalProtocolIdentity() {}

    /** Returns the current version, descriptor digest and rollout identity. */
    public static ProtocolVersion current() {
        return ProtocolVersion.newBuilder().setMajor(PROTOCOL_MAJOR).setMinor(PROTOCOL_MINOR)
                .setDescriptorSetSha256(ByteString.copyFrom(CanonicalProtocolDescriptors.sha256()))
                .setRolloutEpoch(ROLLOUT_EPOCH).build();
    }

    /** Rejects a different wire contract or undeclared protocol identity fields. */
    public static void validate(ProtocolVersion protocol) {
        if (!protocol.getUnknownFields().asMap().isEmpty()
                || protocol.getMajor() != PROTOCOL_MAJOR || protocol.getMinor() != PROTOCOL_MINOR
                || !ROLLOUT_EPOCH.equals(protocol.getRolloutEpoch())
                || !Arrays.equals(CanonicalProtocolDescriptors.sha256(),
                        protocol.getDescriptorSetSha256().toByteArray())) {
            throw new IllegalArgumentException("TaskRequest protocol identity does not match canonical v2");
        }
    }
}
