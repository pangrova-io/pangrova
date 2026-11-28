package org.pangrova.connector.api.metadata;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.session.ConnectorSession;

/** Connector-owned metadata adapter; implementations must not execute rows or load coordinator JNI. */
@Experimental
public interface NativePhysicalWriteProvider {
    /** Validates exact decoded output allocation before a Worker creates any objects. */
    void validateAllocation(ConnectorSession session, ConnectorOutputTableHandle output,
                            NativePhysicalWriteAllocation allocation);
}
