package org.pangrova.spi.resource.admission;

import org.pangrova.spi.annotation.Experimental;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * Context for a query admission decision.
 *
 * <p>Carries the tenant identity, resource group (resolved by the workload
 * classifier), and an optional CBO-estimated memory footprint so the admission
 * controller can enforce memory-aware quotas (not just concurrency counts).</p>
 *
 * @param tenantId        the authenticated tenant (never null or blank)
 * @param resourceGroupId the resource group this query is classified into (never null)
 * @param queryId         unique query identifier (never null or blank)
 * @param estimatedMemoryBytes nonnegative CBO-estimated memory (empty if unavailable)
 */
@Experimental(reason = "G-RES-04: cluster admission control SPI")
public record AdmissionRequest(
        String tenantId,
        String resourceGroupId,
        String queryId,
        OptionalLong estimatedMemoryBytes) {

    public AdmissionRequest {
        Objects.requireNonNull(tenantId, "tenantId must not be null");
        if (tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        Objects.requireNonNull(resourceGroupId, "resourceGroupId must not be null");
        Objects.requireNonNull(queryId, "queryId must not be null");
        if (queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be blank");
        }
        estimatedMemoryBytes = estimatedMemoryBytes == null
                ? OptionalLong.empty() : estimatedMemoryBytes;
        if (estimatedMemoryBytes.isPresent() && estimatedMemoryBytes.getAsLong() < 0) {
            throw new IllegalArgumentException("estimatedMemoryBytes must not be negative");
        }
    }

    /** Creates a request without a memory estimate. */
    public static AdmissionRequest of(String tenantId, String resourceGroupId, String queryId) {
        return new AdmissionRequest(tenantId, resourceGroupId, queryId, OptionalLong.empty());
    }
}
