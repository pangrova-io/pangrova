package org.pangrova.planner.artifact;

import org.pangrova.protocol.v2.ArtifactFormatRef;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.RowLocatorContract;
import org.pangrova.spi.annotation.Internal;

import java.util.Objects;

/** Immutable canonical plan template for one kind of Native artifact work page. */
@Internal
public record ArtifactBuildPlan(
        byte[] planBytes,
        String planNodeId,
        CapabilityRef capability,
        ArtifactFormatRef format,
        RowLocatorContract rowLocator) {

    public ArtifactBuildPlan {
        planBytes = Objects.requireNonNull(planBytes, "planBytes must not be null").clone();
        if (planBytes.length == 0 || planNodeId == null || planNodeId.isBlank()) {
            throw new IllegalArgumentException("artifact build plan identity must not be empty");
        }
        capability = Objects.requireNonNull(capability, "capability must not be null");
        format = Objects.requireNonNull(format, "format must not be null");
        rowLocator = Objects.requireNonNull(rowLocator, "rowLocator must not be null");
    }

    @Override
    public byte[] planBytes() {
        return planBytes.clone();
    }
}
