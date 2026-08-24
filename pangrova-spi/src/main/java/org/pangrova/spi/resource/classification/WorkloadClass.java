package org.pangrova.spi.resource.classification;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;

/**
 * The classification result produced by a {@link WorkloadClassifier}.
 *
 * <p>Contains the resolved resource group id and an optional priority that the
 * admission controller and scheduler may use for ordering.</p>
 *
 * @param resourceGroupId the resource group the query is classified into (never null or blank)
 * @param priority        scheduling priority within the resource group (higher = more urgent; 0 = default)
 */
@Experimental(reason = "G-RES-05: workload classification SPI")
public record WorkloadClass(String resourceGroupId, int priority) {

    /** Default priority (normal). */
    public static final int DEFAULT_PRIORITY = 0;

    public WorkloadClass {
        Objects.requireNonNull(resourceGroupId, "resourceGroupId must not be null");
        if (resourceGroupId.isBlank()) {
            throw new IllegalArgumentException("resourceGroupId must not be blank");
        }
    }

    /** Creates a classification with default priority. */
    public static WorkloadClass of(String resourceGroupId) {
        return new WorkloadClass(resourceGroupId, DEFAULT_PRIORITY);
    }
}
