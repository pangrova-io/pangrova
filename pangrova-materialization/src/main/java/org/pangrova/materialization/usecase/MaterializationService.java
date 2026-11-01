package org.pangrova.materialization.usecase;

import java.time.Duration;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.Principal;

/**
 * Lifecycle use cases of the unified materialization kernel.
 *
 * <p>Every command follows the same protocol: plan and pin sources, reserve an
 * immutable candidate under a live execution fence, execute in the data plane,
 * record exact commit evidence, verify it, and publish with CAS. A failure
 * whose commit outcome cannot be disproved quarantines the candidate instead of
 * pretending the attempt failed cleanly.
 */
public final class MaterializationService {

    private static final Duration DEFAULT_EXECUTION_LEASE = Duration.ofMinutes(30);
    private static final Duration DEFAULT_SOURCE_PIN_TTL = Duration.ofHours(2);

    private final KernelPorts ports;
    private final MaterializationCreationCommands creationCommands;
    private final MaterializationLifecycleCommands lifecycleCommands;

    /** Creates the service with production lease durations. */
    public MaterializationService(KernelPorts ports) {
        this(ports, DEFAULT_EXECUTION_LEASE, DEFAULT_SOURCE_PIN_TTL);
    }

    /** Creates the service with explicit lease durations. */
    public MaterializationService(KernelPorts ports, Duration executionLease, Duration sourcePinTtl) {
        if (ports == null) {
            throw new IllegalArgumentException("ports must not be null");
        }
        if (executionLease == null || executionLease.isZero() || executionLease.isNegative()) {
            throw new IllegalArgumentException("executionLease must be positive");
        }
        if (sourcePinTtl == null || sourcePinTtl.isZero() || sourcePinTtl.isNegative()) {
            throw new IllegalArgumentException("sourcePinTtl must be positive");
        }
        this.ports = ports;
        MaterializationAttemptExecutor attemptExecutor = new MaterializationAttemptExecutor(
                ports, executionLease, sourcePinTtl);
        RefreshResolutionPolicy resolutionPolicy = new RefreshResolutionPolicy(ports.extensions());
        this.creationCommands = new MaterializationCreationCommands(
                ports, attemptExecutor, resolutionPolicy);
        this.lifecycleCommands = new MaterializationLifecycleCommands(
                ports, attemptExecutor, resolutionPolicy);
    }

    /** Creates an object and publishes its first generation. */
    public MaterializationOutcome create(CreateMaterialization command) {
        return creationCommands.create(command);
    }

    /** Replaces an existing object atomically through a definition revision. */
    public MaterializationOutcome createOrReplace(CreateMaterialization command) {
        return creationCommands.createOrReplace(command);
    }

    /** Switches refresh mode and rebuilds the published head with a full refresh. */
    public MaterializationOutcome setRefreshMode(SetRefreshMode command) {
        return lifecycleCommands.setRefreshMode(command);
    }

    /** Renames an object without touching its data or generation identity. */
    public void rename(RenameMaterialization command) {
        lifecycleCommands.rename(command);
    }

    /** Recomputes an active object and publishes the new generation. */
    public MaterializationOutcome refresh(RefreshMaterialization command) {
        return lifecycleCommands.refresh(command);
    }

    /** Records a new definition revision; the next refresh publishes it. */
    public void redefine(RedefineMaterialization command) {
        lifecycleCommands.redefine(command);
    }

    /** Drops the logical name; physical generations remain until GC proves them unpinned. */
    public void drop(DropMaterialization command) {
        lifecycleCommands.drop(command);
    }

    /** Suspends automatic refresh; already suspended objects are unchanged. */
    public void suspend(SuspendMaterialization command) {
        lifecycleCommands.suspend(command);
    }

    /** Resumes automatic refresh; already active objects are unchanged. */
    public void resume(ResumeMaterialization command) {
        lifecycleCommands.resume(command);
    }

    /** Changes the freshness target, activating a manual schedule. */
    public void setTargetLag(SetTargetLag command) {
        lifecycleCommands.setTargetLag(command);
    }

    /** Checks SEL/read authority on one materialization. */
    public void requireRead(MaterializationId object, Principal requester) {
        ports.authorizer().requireRead(object, requester);
    }
}
