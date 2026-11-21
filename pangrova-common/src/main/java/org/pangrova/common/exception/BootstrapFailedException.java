package org.pangrova.common.exception;

/**
 * Thrown when a bootstrap phase fails, halting the startup sequence.
 *
 * <p>Shared across coordinator and worker bootstrap paths. The {@code phase}
 * field identifies which specific phase failed (e.g., "HTTP", "Metadata",
 * "Transport", "registration").</p>
 */
public final class BootstrapFailedException extends RuntimeException {

    private final String phase;

    public BootstrapFailedException(String phase, Throwable cause) {
        super("Bootstrap phase '" + phase + "' failed: " + cause.getMessage(), cause);
        this.phase = phase;
    }

    public String getPhase() {
        return phase;
    }
}
