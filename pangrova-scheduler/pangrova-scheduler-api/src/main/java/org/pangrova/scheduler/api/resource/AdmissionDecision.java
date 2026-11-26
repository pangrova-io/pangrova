package org.pangrova.scheduler.api.resource;

import org.pangrova.scheduler.api.enums.AdmissionVerdict;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;

/**
 * The outcome of a {@link org.pangrova.scheduler.api.admission.ClusterAdmissionController#admit} call.
 *
 * <p>Contains a verdict (ADMIT / REJECT) and a human-readable reason
 * for observability and client feedback.</p>
 *
 * @param verdict the admission verdict
 * @param reason  human-readable justification (e.g. "concurrent query limit reached")
 */
@Experimental(reason = "G-RES-04: cluster admission control SPI")
public record AdmissionDecision(AdmissionVerdict verdict, String reason) {

    private static final AdmissionDecision ADMITTED = new AdmissionDecision(AdmissionVerdict.ADMIT, "within quota");

    public AdmissionDecision {
        Objects.requireNonNull(verdict, "verdict must not be null");
        Objects.requireNonNull(reason, "reason must not be null");
    }

    /** Convenience factory for an ADMIT decision. */
    public static AdmissionDecision admit() {
        return ADMITTED;
    }

    /** Convenience factory for a REJECT decision. */
    public static AdmissionDecision reject(String reason) {
        return new AdmissionDecision(AdmissionVerdict.REJECT, reason);
    }

    /** @return true if the query may proceed to scheduling. */
    public boolean isAdmitted() {
        return verdict == AdmissionVerdict.ADMIT;
    }
}
