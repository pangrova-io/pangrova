package org.pangrova.shuffle.api.capability;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.shuffle.api.enums.ShuffleCapability;

/**
 * Immutable, explicit Shuffle guarantees. An absent capability is unsupported, never inferred.
 *
 * <p>A provider descriptor is only an upper bound. Admission and recovery must intersect it with
 * deployment guarantees and the evidence for the particular exchange. An uncommitted exchange,
 * expired retention or an unselected attempt cannot acquire a guarantee from a provider name,
 * local disk, PUSH/PULL transport or this value alone. This value owns no state machine, lease,
 * credential or resource and is not a commit receipt or a decommission authorization.
 *
 * @param capabilities independently supported guarantees; copied, without null elements
 */
@Experimental(reason = "Provider session and exchange evidence bindings are being consolidated")
public record ShuffleCapabilityDescriptor(Set<ShuffleCapability> capabilities) {
    public ShuffleCapabilityDescriptor {
        capabilities = Set.copyOf(Objects.requireNonNull(capabilities, "capabilities must not be null"));
    }

    /** Unknown or unsupported guarantees; suitable for an implementation with no verified contract. */
    public static ShuffleCapabilityDescriptor none() {
        return new ShuffleCapabilityDescriptor(Set.of());
    }

    /** Declares exactly the supplied guarantees, without deriving additional guarantees. */
    public static ShuffleCapabilityDescriptor of(ShuffleCapability... capabilities) {
        Objects.requireNonNull(capabilities, "capabilities must not be null");
        var declared = EnumSet.noneOf(ShuffleCapability.class);
        for (ShuffleCapability capability : capabilities) {
            declared.add(Objects.requireNonNull(capability, "capability must not be null"));
        }
        return new ShuffleCapabilityDescriptor(declared);
    }

    public boolean supports(ShuffleCapability capability) {
        return capabilities.contains(Objects.requireNonNull(capability, "capability must not be null"));
    }

    /** Restricts guarantees; this operation can never introduce a capability. */
    public ShuffleCapabilityDescriptor intersect(ShuffleCapabilityDescriptor other) {
        Objects.requireNonNull(other, "other must not be null");
        var common = EnumSet.noneOf(ShuffleCapability.class);
        common.addAll(capabilities);
        common.retainAll(other.capabilities);
        return new ShuffleCapabilityDescriptor(common);
    }
}
