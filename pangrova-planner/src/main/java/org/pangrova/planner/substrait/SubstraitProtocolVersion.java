package org.pangrova.planner.substrait;

import io.substrait.proto.Plan;
import io.substrait.proto.Version;

/**
 * Defines the Substrait protocol version and producer identity that Pangrova
 * embeds into every serialized Plan, and the validation rules consumers
 * apply on receipt.
 *
 * <p><b>Why this exists.</b> Pangrova's coordinator (producer) and native
 * worker (consumer) both pin {@code substrait-java 0.23.0} via the build,
 * but the proto descriptors are physically duplicated on the C++ side
 * (under {@code pangrova-worker-native/proto/substrait/}). If the two ever
 * drift, errors surface deep inside conversion as opaque "field N expected
 * type X" decode failures instead of at the boundary. Embedding a
 * {@link Version} sub-message and a producer string makes drift detectable
 * at the receive boundary with a clear, actionable error.
 *
 * <p><b>Compatibility rule.</b> Major-version mismatch is rejected. Minor
 * and patch differences are accepted (Substrait spec semver: minor bumps
 * are additive). The native consumer applies the matching rule in
 * {@code pangrova-worker-native/src/velox/plan/NativePlanConverter.cpp};
 * keep the two in sync (the constants here are the source of truth).
 *
 * <p>Reference: the Substrait {@code Plan} message defines a {@code
 * Version} field that became required for Substrait spec >= 0.17.0 (see
 * {@code substrait/plan.proto}). The {@code git_hash} field is omitted
 * here — Pangrova targets release tags, not unreleased commits.
 */
public final class SubstraitProtocolVersion {

    /** Substrait spec major version Pangrova targets. */
    public static final int SPEC_MAJOR = 0;

    /** Substrait spec minor version Pangrova targets. */
    public static final int SPEC_MINOR = 51;

    /** Substrait spec patch version Pangrova targets. */
    public static final int SPEC_PATCH = 0;

    /**
     * Producer identifier embedded in every Plan. Substrait reserves this
     * field for consumer-side bug-workaround diagnostics: a consumer that
     * sees an unfamiliar producer can refuse the plan or relax checks.
     * Format: {@code "<product>/<version>"}.
     */
    public static final String PRODUCER = "Pangrova/0.1.0";

    private SubstraitProtocolVersion() {}

    /**
     * Build the {@link Version} sub-message that should be set on every
     * outgoing Plan.
     */
    public static Version build() {
        return Version.newBuilder()
                .setMajorNumber(SPEC_MAJOR)
                .setMinorNumber(SPEC_MINOR)
                .setPatchNumber(SPEC_PATCH)
                .setProducer(PRODUCER)
                .build();
    }

    /**
     * Stamp the version + producer into the supplied Plan builder. Used by
     * the serializer immediately after substrait-java's
     * {@code PlanProtoConverter.toProto()} (which does not populate the
     * version field).
     */
    public static void stamp(Plan.Builder builder) {
        builder.setVersion(build());
    }

    /**
     * Validate the version embedded in a received Plan. Throws
     * {@link SubstraitVersionMismatchException} on incompatibility (no
     * version, missing producer, or major-version mismatch). Mirror of the
     * native consumer's checks in {@code NativePlanConverter.cpp}.
     *
     * @param plan parsed Substrait Plan to validate
     */
    public static void validate(Plan plan) {
        if (!plan.hasVersion()) {
            throw new SubstraitVersionMismatchException(
                    "Substrait Plan is missing required Version sub-message; "
                            + "expected producer=\"" + PRODUCER
                            + "\" version=" + SPEC_MAJOR + "." + SPEC_MINOR + "." + SPEC_PATCH
                            + ". Producer and consumer have drifted.");
        }
        Version v = plan.getVersion();
        if (v.getProducer().isEmpty()) {
            throw new SubstraitVersionMismatchException(
                    "Substrait Plan Version.producer is empty; refusing to "
                            + "execute plan from unidentified producer (Pangrova requires every "
                            + "plan to carry a producer string).");
        }
        if (v.getMajorNumber() != SPEC_MAJOR) {
            throw new SubstraitVersionMismatchException(
                    "Substrait spec major-version mismatch: plan declares "
                            + v.getMajorNumber() + "." + v.getMinorNumber() + "." + v.getPatchNumber()
                            + " (producer=\"" + v.getProducer() + "\") but consumer requires major="
                            + SPEC_MAJOR + ". Major bumps are breaking — rebuild producer "
                            + "and consumer to the same Substrait SDK version.");
        }
        // Minor/patch differences are allowed: Substrait minor bumps are
        // additive. Higher-than-expected minor on the producer side is
        // logged by the caller (planner emits a debug event); we don't
        // reject here.
    }
}
