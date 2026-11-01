package org.pangrova.spi.function.runtime;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Projects a capability failure raised inside a Native function callback into
 * canonical Native task error bytes.
 *
 * <p>The Worker function bridge re-raises the original callback failure to the
 * native engine. A capability provider installs this projector so the engine
 * can record the canonical error contract instead of a generic function
 * failure. Without an installed projector the bridge reports no projection and
 * the native engine handles the failure as an ordinary function error.</p>
 */
@Experimental(reason = "G-SPI-08: function/UDF plugin SPI")
public interface FunctionFailureProjector {

    /**
     * Projects the failure, or returns {@code null} when the failure does not
     * belong to the projector's capability domain.
     *
     * @param failure the failure observed by the Native callback
     * @return canonical {@code ExecutionError} bytes, or {@code null}
     */
    byte[] describeFailure(Throwable failure);
}
