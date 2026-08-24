/**
 * Hardware accelerator offload SPI for the Pangrova execution engine.
 *
 * <p>This package defines the extension points that allow pluggable hardware
 * backends (GPU/CUDA, OpenCL, FPGA, TPU) to be registered via
 * {@link java.util.ServiceLoader} and selected at runtime through the
 * {@code execution.accelerator} configuration property.</p>
 *
 * <p>The default accelerator type is {@link org.pangrova.spi.execution.accelerator.AcceleratorType#CPU},
 * which represents the current native vectorized execution engine on CPU path. All types in this
 * package are {@link org.pangrova.spi.annotation.Experimental @Experimental}.</p>
 *
 * @see org.pangrova.spi.execution.accelerator.AcceleratorProvider
 * @see org.pangrova.spi.execution.accelerator.AcceleratorSession
 */
package org.pangrova.spi.execution.accelerator;
