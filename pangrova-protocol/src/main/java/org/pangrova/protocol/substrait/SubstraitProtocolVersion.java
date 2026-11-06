package org.pangrova.protocol.substrait;

import io.substrait.proto.Plan;
import io.substrait.proto.Version;

/**
 * Wire version stamped by Java plan producers and validated on the Java generation
 * replay path. This owns only protocol metadata, not planning or execution routing.
 * The current policy requires a producer and the configured major version, while
 * accepting minor/patch differences. Native retrieval preparation and plan conversion
 * enforce the same policy through {@code engine/plan/SubstraitProtocolVersion.h}.
 * Packaged JNI tests verify both rejection and compatibility across the boundary.
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
     * version, missing producer, or major-version mismatch). Used by Java producers and durable replay before dispatch.
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
        // Minor/patch differences are allowed. Individual relation/function
        // support is validated separately; version acceptance is not a promise
        // that every feature in a newer producer is executable.
    }
}
