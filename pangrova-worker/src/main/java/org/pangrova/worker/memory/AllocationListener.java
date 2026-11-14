package org.pangrova.worker.memory;

/**
 * Callback interface for Native-side memory arbitration.
 *
 * <p>Called synchronously from C++ via JNI within a Java-initiated call chain.
 * The calling thread is always the same Java thread that entered Native code,
 * so implementations must not block on locks that could deadlock with Native
 * arbitration.
 *
 * <p>Placement: memory/ — Java-side memory coordination contracts.
 */
public interface AllocationListener {

    /**
     * Reserve memory for a Native allocation request.
     *
     * @param bytes requested byte count (positive)
     * @return actual bytes granted (0 = denied, caller should trigger spill)
     */
    long reserve(long bytes);

    /**
     * Release previously reserved memory back to the pool.
     *
     * @param bytes byte count to release (positive)
     */
    void unreserve(long bytes);
}
