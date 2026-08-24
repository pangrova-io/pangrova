package org.pangrova.spi.execution.accelerator;

import org.pangrova.spi.annotation.Experimental;

/**
 * {@code ServiceLoader} plugin point for hardware accelerator backends.
 *
 * <p>Implementations are discovered at runtime via {@link java.util.ServiceLoader}
 * and selected by the {@code execution.accelerator} configuration property
 * (values match {@link AcceleratorType}: {@code cpu}, {@code gpu_cuda},
 * {@code gpu_opencl}, {@code fpga}, {@code tpu}).</p>
 *
 * <p>A provider must be able to report availability without side-effects so
 * that the bootstrap can probe all registered providers and fall back to
 * {@link AcceleratorType#CPU} when no accelerator hardware is detected.</p>
 *
 * <p>Lifecycle: the engine calls {@link #isAvailable()} first; only providers
 * that return {@code true} may have {@link #createSession()} called. Sessions
 * hold device resources and must be closed by the caller.</p>
 */
@Experimental(reason = "G-EXEC-08 hardware accelerator offload SPI")
public interface AcceleratorProvider {

    /**
     * Returns the accelerator type this provider implements.
     *
     * @return the accelerator type, never null
     */
    AcceleratorType type();

    /**
     * Returns whether the accelerator hardware is present and usable on
     * this node.
     *
     * <p>Implementations should perform a lightweight probe (e.g. check for
     * the CUDA runtime library) without allocating heavyweight resources.</p>
     *
     * @return true if the accelerator can be used
     */
    boolean isAvailable();

    /**
     * Describes the capabilities of this accelerator.
     *
     * @return the capabilities, never null
     * @throws IllegalStateException if called when {@link #isAvailable()} is false
     */
    AcceleratorCapabilities capabilities();

    /**
     * Creates a new execution session on the accelerator device.
     *
     * <p>The caller is responsible for closing the returned session to
     * release device resources.</p>
     *
     * @return a new session, never null
     * @throws IllegalStateException if called when {@link #isAvailable()} is false
     */
    AcceleratorSession createSession();
}
