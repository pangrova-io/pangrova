package org.pangrova.spi.execution.accelerator;

import org.pangrova.spi.annotation.Experimental;

/**
 * Enumerates the hardware accelerator backends that an {@link AcceleratorProvider}
 * may target.
 *
 * <p>{@link #CPU} represents the default execution path (the native vectorized execution engine on x86/ARM); the
 * remaining entries model offload targets that future providers may implement.
 * The scheduler consults the provider's declared type to route operators to the
 * appropriate backend.</p>
 */
@Experimental(reason = "G-EXEC-08 hardware accelerator offload SPI")
public enum AcceleratorType {

    /** Default CPU-based execution (current native engine path). */
    CPU,

    /** NVIDIA CUDA GPU offload. */
    GPU_CUDA,

    /** Vendor-neutral OpenCL GPU offload. */
    GPU_OPENCL,

    /** Field-Programmable Gate Array offload. */
    FPGA,

    /** Tensor Processing Unit offload (e.g. Google TPU). */
    TPU
}
