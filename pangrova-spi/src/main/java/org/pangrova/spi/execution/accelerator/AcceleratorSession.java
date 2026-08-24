package org.pangrova.spi.execution.accelerator;

import java.nio.ByteBuffer;

import org.pangrova.spi.annotation.Experimental;

/**
 * A session for executing operators on a hardware accelerator.
 *
 * <p>Sessions are obtained from {@link AcceleratorProvider#createSession()} and
 * must be closed after use to release device resources (GPU memory, FPGA
 * contexts, etc.). Implementations must be safe for use from a single thread;
 * callers requiring concurrent offload should create multiple sessions.</p>
 *
 * <p>Input and output are exchanged as {@link ByteBuffer}s carrying
 * Arrow-compatible columnar batches. The concrete serialization format is
 * provider-specific and documented per implementation.</p>
 */
@Experimental(reason = "G-EXEC-08 hardware accelerator offload SPI")
public interface AcceleratorSession extends AutoCloseable {

    /**
     * Executes an operator on the accelerator with the given input batch.
     *
     * @param operatorName the name of the operator to execute (e.g. {@code "hash_join"})
     * @param input        the input data as an Arrow-compatible columnar batch
     * @return the result batch
     * @throws UnsupportedOperationException if the operator is not supported
     * @throws IllegalStateException         if the session has been closed
     * @throws NullPointerException          if operatorName or input is null
     */
    ByteBuffer execute(String operatorName, ByteBuffer input);

    /**
     * Releases all device resources held by this session.
     *
     * <p>After close, calling {@link #execute} must throw
     * {@link IllegalStateException}.</p>
     */
    @Override
    void close();
}
