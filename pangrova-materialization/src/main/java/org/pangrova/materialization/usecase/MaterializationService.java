package org.pangrova.materialization.usecase;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.QueryId;
import org.pangrova.materialization.domain.ReaderPin;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.SourcePin;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.extension.RefreshContext;
import org.pangrova.materialization.domain.RefreshPlan;
import org.pangrova.materialization.extension.RefreshStrategy;
import org.pangrova.materialization.extension.StrategyCompatibility;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.ExecutionRequest;
import org.pangrova.materialization.port.ExecutionResult;
import org.pangrova.materialization.port.MaterializationAction;
import org.pangrova.materialization.port.MaterializationEvent;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;
import org.pangrova.materialization.port.PublicationResult;
import org.pangrova.materialization.port.PublicationVerification;
import org.pangrova.materialization.port.PublishCommand;
import org.pangrova.spi.materialization.ChangeProof;
import org.pangrova.spi.materialization.ChangeSetMode;
import org.pangrova.spi.materialization.ChangeSetPlan;
import org.pangrova.spi.materialization.CommitEvidence;
import org.pangrova.spi.materialization.MaterializationSourceException;
import org.pangrova.spi.materialization.PinnedSource;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;

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
    private final Duration executionLease;
    private final Duration sourcePinTtl;

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
        this.executionLease = executionLease;
        this.sourcePinTtl = sourcePinTtl;
    }

    /** Creates an object and publishes its first generation. */
    public MaterializationOutcome create(CreateMaterialization command) {
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            return resumeCreate(command);
        }
        Optional<MaterializationObject> existing = ports.repository().findObject(command.object());
        if (existing.isPresent()) {
            if (command.ifNotExists()) {
                return existingOutcome(command, existing.get());
            }
            throw MaterializationException.requestRejected(
                    "materialization " + command.object().canonicalName() + " already exists");
        }
        SecurityModeGuard.require(command.securityMode());
        Instant now = ports.clock().now();
        PlannedDefinition planned = plan(command, 1);
        requireRefreshConfiguration(command.refreshMode(), command.incrementalStrategy(),
                planned.supportedRefreshKinds());
        ports.authorizer().requireCreate(
                command.object(), command.kind(), command.requester(), planned.sources());

        MaterializationObject header = MaterializationObject.creating(
                command.object(), command.kind(), new org.pangrova.materialization.domain.IncarnationId(
                        ports.ids().newId()), command.definer(), command.securityMode(),
                command.refreshMode(), command.incrementalStrategy(), now);
        DefinitionRevision definition = new DefinitionRevision(
                command.object(), 1, command.definitionSql(), planned.binding(),
                planned.outputSchema(), planned.sources(), planned.dependencies(),
                planned.policySnapshot(), command.securityMode(), command.definer(),
                command.sessionProperties(), now);
        Schedule schedule = command.targetLag()
                .map(lag -> Schedule.scheduled(command.object(), lag, lag.deadlineFrom(now)))
                .orElseGet(() -> Schedule.manual(command.object()));
        boolean created = ports.repository().createObjectIfAbsent(header, definition, schedule,
                command.commandId(),
                event(MaterializationEventType.OBJECT_CREATED, command.object(), command.requester(),
                        command.definer(), command.commandId(), Optional.empty(),
                        Optional.empty(), "created " + command.kind(), now));
        if (!created) {
            MaterializationObject conflicting = ports.repository().findObject(command.object())
                    .orElseThrow(() -> MaterializationException.repositoryFailure(
                            "a conflicting create disappeared for "
                                    + command.object().canonicalName(), null));
            if (command.ifNotExists()) {
                return existingOutcome(command, conflicting);
            }
            throw MaterializationException.requestRejected(
                    "materialization " + command.object().canonicalName() + " already exists");
        }

        return executeAttempt(header, definition, planned, command.requester(),
                command.sessionProperties(), command.commandId(), TriggerKind.CREATION,
                fullResolution());
    }

    /**
     * Replaces an existing object atomically through a definition revision.
     *
     * <p>The old published head stays readable until the forced full refresh of
     * the replacement succeeds. A failed replacement therefore never exposes
     * partial data; retrying the command resumes the pending publication.
     */
    public MaterializationOutcome createOrReplace(CreateMaterialization command) {
        if (command.ifNotExists()) {
            throw MaterializationException.requestRejected(
                    "OR REPLACE and IF NOT EXISTS are mutually exclusive");
        }
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            return resumeReplace(command);
        }
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElse(null);
        if (header == null) {
            return create(command);
        }
        if (header.kind() != command.kind()) {
            throw MaterializationException.requestRejected(
                    "materialization " + command.object().canonicalName()
                            + " already exists as " + header.kind());
        }
        if (header.state() != ObjectState.Active.INSTANCE
                && header.state() != ObjectState.Suspended.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "replace requires an active or suspended materialization, found "
                            + header.state().stateName());
        }
        SecurityModeGuard.require(command.securityMode());
        long nextRevision = header.definitionRevision() + 1;
        PlannedDefinition planned = plan(command, nextRevision);
        requireRefreshConfiguration(command.refreshMode(), command.incrementalStrategy(),
                planned.supportedRefreshKinds());
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.ALTER);
        ports.authorizer().requireCreate(command.object(), command.kind(), command.requester(),
                planned.sources());
        Instant now = ports.clock().now();
        DefinitionRevision definition = new DefinitionRevision(
                command.object(), nextRevision, command.definitionSql(), planned.binding(),
                planned.outputSchema(), planned.sources(), planned.dependencies(),
                planned.policySnapshot(), command.securityMode(), command.definer(),
                command.sessionProperties(), now);
        MaterializationObject replacement = header.withReplacement(nextRevision,
                command.refreshMode(), command.incrementalStrategy(), header.revision(), now);
        ports.repository().replaceDefinition(definition, replacement, header.revision(),
                command.commandId(),
                event(MaterializationEventType.OBJECT_ALTERED, command.object(),
                        command.requester(), header.definer(), command.commandId(),
                        Optional.empty(), Optional.empty(),
                        "replaced definition at revision " + nextRevision, now));
        command.targetLag().ifPresent(lag -> casSchedule(command.object(),
                schedule -> schedule.changeTargetLag(lag, now)));
        return executeAttempt(replacement, definition, planned, command.requester(),
                command.sessionProperties(), command.commandId(), TriggerKind.REPAIR,
                fullResolution());
    }

    /**
     * Switches refresh mode and rebuilds the published head with a full refresh.
     *
     * <p>The configuration is committed only after the forced full generation
     * publishes, so a failed rebuild leaves the previous mode and head intact.
     * A retry after a crash between publication and configuration update applies
     * the remaining configuration change without recomputing data.
     */
    public MaterializationOutcome setRefreshMode(SetRefreshMode command) {
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        Optional<StrategyId> strategy = command.mode() == RefreshMode.INCREMENTAL
                ? command.strategy().or(() -> header.incrementalStrategy())
                : Optional.empty();
        if (header.refreshMode() == command.mode()
                && header.incrementalStrategy().equals(strategy)) {
            return replay(command.object());
        }
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            return applyRefreshConfiguration(command, strategy, header, false);
        }
        if (header.state() != ObjectState.Active.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "refresh mode change requires an active materialization, found "
                            + header.state().stateName());
        }
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.ALTER);
        DefinitionRevision definition = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        PlannedDefinition planned = planAndValidate(command.object(), header.kind(), definition,
                header.definer(), definition.sessionProperties());
        requireRefreshConfiguration(command.mode(), strategy,
                planned.supportedRefreshKinds());
        MaterializationOutcome rebuilt = executeAttempt(header, definition, planned,
                command.requester(), definition.sessionProperties(), command.commandId(),
                TriggerKind.REPAIR, fullResolution());
        return applyRefreshConfiguration(command, strategy, rebuilt.object(), true);
    }

    private MaterializationOutcome applyRefreshConfiguration(SetRefreshMode command,
            Optional<StrategyId> strategy, MaterializationObject header, boolean rebuilt) {
        Instant now = ports.clock().now();
        MaterializationObject configured = header.withRefreshConfiguration(
                command.mode(), strategy, header.revision(), now);
        ports.repository().updateObject(configured, header.revision(), command.commandId(),
                event(MaterializationEventType.OBJECT_ALTERED, command.object(),
                        command.requester(), header.definer(), command.commandId(),
                        Optional.empty(), Optional.empty(),
                        "refresh mode " + command.mode() + strategy
                                .map(value -> ", strategy " + value.value()).orElse(""),
                        now));
        Generation published = ports.repository().findPublishedGeneration(command.object())
                .orElseThrow(() -> MaterializationException.invalidStateTransition(
                        "configured materialization has no published generation: "
                                + command.object().canonicalName()));
        return new MaterializationOutcome(configured, published, rebuilt);
    }

    /**
     * Renames an object without touching its data or generation identity.
     *
     * <p>A rename is rejected while live materializations consume the object:
     * their submitted SQL still spells the old name, so allowing the rename
     * would make their next refresh fail. Drop those dependents first.
     */
    public void rename(RenameMaterialization command) {
        if (ports.repository().findCommand(command.object().tenant(), command.commandId())
                .isPresent()) {
            return;
        }
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        if (header.state() != ObjectState.Active.INSTANCE
                && header.state() != ObjectState.Suspended.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "rename requires an active or suspended materialization, found "
                            + header.state().stateName());
        }
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.ALTER);
        List<MaterializationId> dependents = ports.repository()
                .findLiveDependents(command.object());
        if (!dependents.isEmpty()) {
            throw MaterializationException.dependencyConflict(
                    "cannot rename " + command.object().canonicalName() + " while "
                            + dependents.stream().map(MaterializationId::canonicalName)
                                    .collect(java.util.stream.Collectors.joining(", "))
                            + " depends on it");
        }
        if (ports.repository().findObject(command.newObject()).isPresent()) {
            throw MaterializationException.requestRejected(
                    "materialization " + command.newObject().canonicalName() + " already exists");
        }
        Instant now = ports.clock().now();
        MaterializationObject renamed = header.renamed(command.newObject(), header.revision(), now);
        ports.repository().renameObject(renamed, header.id(), header.revision(),
                command.commandId(),
                event(MaterializationEventType.OBJECT_ALTERED, command.newObject(),
                        command.requester(), header.definer(), command.commandId(),
                        Optional.empty(), Optional.empty(),
                        "renamed from " + header.id().canonicalName(), now));
    }

    private MaterializationOutcome existingOutcome(CreateMaterialization command,
            MaterializationObject header) {
        if (header.kind() != command.kind()) {
            throw MaterializationException.requestRejected(
                    "materialization " + command.object().canonicalName()
                            + " already exists as " + header.kind());
        }
        if (header.publishedGeneration().isEmpty()) {
            throw MaterializationException.requestRejected(
                    "materialization " + command.object().canonicalName()
                            + " already exists in state " + header.state().stateName());
        }
        Generation published = ports.repository().findPublishedGeneration(command.object())
                .orElseThrow(() -> MaterializationException.repositoryFailure(
                        "existing materialization has no published generation: "
                                + command.object().canonicalName(), null));
        return new MaterializationOutcome(header, published, false);
    }

    private MaterializationOutcome resumeReplace(CreateMaterialization command) {
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        if (header.kind() != command.kind()) {
            throw MaterializationException.requestRejected(
                    "materialization " + command.object().canonicalName()
                            + " already exists as " + header.kind());
        }
        Optional<Generation> published = ports.repository()
                .findPublishedGeneration(command.object());
        if (published.isPresent()
                && published.get().definitionRevision() == header.definitionRevision()) {
            return new MaterializationOutcome(header, published.get(), false);
        }
        DefinitionRevision definition = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        PlannedDefinition planned = planAndValidate(command.object(), header.kind(), definition,
                command.definer(), definition.sessionProperties());
        return executeAttempt(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), TriggerKind.REPAIR,
                fullResolution());
    }

    /** Recomputes an active object and publishes the new generation. */
    public MaterializationOutcome refresh(RefreshMaterialization command) {
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            return replay(command.object());
        }
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        if (header.state() != ObjectState.Active.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "refresh requires an active materialization, found " + header.state().stateName());
        }
        Generation published = ports.repository().findPublishedGeneration(command.object())
                .orElseThrow(() -> MaterializationException.invalidStateTransition(
                        "active object has no published generation: " + command.object().canonicalName()));
        DefinitionRevision definition = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.REFRESH);

        PlannedDefinition planned = planAndValidate(command.object(), header.kind(), definition,
                header.definer(), definition.sessionProperties());
        RefreshResolution resolution = resolveRefresh(header, planned, published);
        return executeAttempt(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), command.trigger(), resolution);
    }

    /** Records a new definition revision; the next refresh publishes it. */
    public void redefine(RedefineMaterialization command) {
        if (ports.repository().findCommand(command.object().tenant(), command.commandId())
                .isPresent()) {
            return;
        }
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        if (header.state() != ObjectState.Active.INSTANCE
                && header.state() != ObjectState.Suspended.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "redefine requires an active or suspended materialization, found "
                            + header.state().stateName());
        }
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.ALTER);
        long nextRevision = header.definitionRevision() + 1;
        PlannedDefinition planned = ports.planner().plan(new PlanningRequest(
                command.object(), header.kind(), command.definitionSql(), header.definer(),
                nextRevision, command.sessionProperties()));
        Instant now = ports.clock().now();
        DefinitionRevision definition = new DefinitionRevision(
                command.object(), nextRevision, command.definitionSql(), planned.binding(),
                planned.outputSchema(), planned.sources(), planned.dependencies(),
                planned.policySnapshot(), header.securityMode(), header.definer(),
                command.sessionProperties(), now);
        MaterializationObject redefined =
                header.withDefinitionRevision(nextRevision, header.revision(), now);
        ports.repository().replaceDefinition(definition, redefined, header.revision(),
                command.commandId(),
                event(MaterializationEventType.OBJECT_ALTERED, command.object(),
                        command.requester(), header.definer(), command.commandId(),
                        Optional.empty(), Optional.empty(),
                        "redefined at revision " + nextRevision, now));
    }

    /** Drops the logical name; physical generations remain until GC proves them unpinned. */
    public void drop(DropMaterialization command) {
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElse(null);
        if (header == null) {
            if (command.ifExists()) {
                return;
            }
            throw MaterializationException.notFound(command.object());
        }
        if (header.state() == ObjectState.Dropped.INSTANCE) {
            return;
        }
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.DROP);
        List<MaterializationId> order = new ArrayList<>();
        collectDependentsFirst(command.object(), new java.util.LinkedHashSet<>(), order);
        if (order.size() > 1 && !command.cascade()) {
            order.remove(command.object());
            throw MaterializationException.dependencyConflict(
                    "materialization " + command.object().canonicalName() + " is referenced by "
                            + order.stream().map(MaterializationId::canonicalName)
                                    .collect(java.util.stream.Collectors.joining(", "))
                            + "; drop the dependents first or use CASCADE");
        }
        for (MaterializationId target : order) {
            if (!target.equals(command.object())) {
                ports.authorizer().requireManage(target, command.requester(),
                        MaterializationAction.DROP);
            }
        }
        for (MaterializationId target : order) {
            dropOne(target, command);
        }
    }

    private void collectDependentsFirst(MaterializationId object,
            Set<MaterializationId> visited, List<MaterializationId> order) {
        if (!visited.add(object)) {
            return;
        }
        for (MaterializationId dependent : ports.repository().findLiveDependents(object)) {
            collectDependentsFirst(dependent, visited, order);
        }
        order.add(object);
    }

    private void dropOne(MaterializationId object, DropMaterialization command) {
        MaterializationObject header = ports.repository().findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        if (header.state() == ObjectState.Dropped.INSTANCE) {
            return;
        }
        suspendSchedule(object);
        Instant now = ports.clock().now();
        MaterializationObject dropping = header;
        if (header.state() != ObjectState.Dropping.INSTANCE) {
            dropping = header.transitionTo(ObjectState.Dropping.INSTANCE, header.revision(), now);
            ports.repository().updateObject(dropping, header.revision(), command.commandId(),
                    event(MaterializationEventType.OBJECT_ALTERED, object,
                            command.requester(), header.definer(), command.commandId(),
                            Optional.empty(), Optional.empty(), "dropping", now));
        }
        MaterializationObject dropped = dropping.transitionTo(
                ObjectState.Dropped.INSTANCE, dropping.revision(), now);
        ports.repository().updateObject(dropped, dropping.revision(), command.commandId(),
                event(MaterializationEventType.OBJECT_DROPPED, object,
                        command.requester(), header.definer(), command.commandId(),
                        Optional.empty(), Optional.empty(), "dropped", now));
    }

    /** Suspends automatic refresh; already suspended objects are unchanged. */
    public void suspend(SuspendMaterialization command) {
        requireAlterable(command.object(), command.requester());
        casSchedule(command.object(), schedule -> schedule.state()
                == org.pangrova.materialization.domain.ScheduleState.ACTIVE
                ? schedule.suspend() : schedule);
    }

    /** Resumes automatic refresh; already active objects are unchanged. */
    public void resume(ResumeMaterialization command) {
        requireAlterable(command.object(), command.requester());
        casSchedule(command.object(), schedule -> switch (schedule.state()) {
            case ACTIVE -> schedule;
            case SUSPENDED -> schedule.resume(ports.clock().now());
            case DISABLED -> throw MaterializationException.invalidStateTransition(
                    "materialization " + command.object().canonicalName()
                            + " has no target lag to resume");
        });
    }

    /** Changes the freshness target, activating a manual schedule. */
    public void setTargetLag(SetTargetLag command) {
        requireAlterable(command.object(), command.requester());
        casSchedule(command.object(),
                schedule -> schedule.changeTargetLag(command.targetLag(), ports.clock().now()));
    }

    private void requireAlterable(MaterializationId object, Principal requester) {
        MaterializationObject header = ports.repository().findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        if (header.state() == ObjectState.Dropping.INSTANCE
                || header.state() == ObjectState.Dropped.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "materialization " + object.canonicalName() + " is " + header.state().stateName());
        }
        ports.authorizer().requireManage(object, requester, MaterializationAction.ALTER);
    }

    private Schedule casSchedule(MaterializationId object,
            java.util.function.UnaryOperator<Schedule> mutation) {
        for (int attempt = 0; attempt < 3; attempt++) {
            Schedule current = ports.repository().findSchedule(object)
                    .orElseThrow(() -> MaterializationException.requestRejected(
                            "materialization " + object.canonicalName() + " has no schedule"));
            Schedule updated = mutation.apply(current);
            if (updated.version() == current.version()) {
                return current;
            }
            try {
                ports.repository().updateSchedule(updated, current.version());
                return updated;
            } catch (MaterializationException conflict) {
                if (conflict.getErrorCode()
                        != org.pangrova.common.exception.PangrovaErrorCode
                                .MATERIALIZATION_CONCURRENT_PUBLICATION) {
                    throw conflict;
                }
            }
        }
        throw MaterializationException.concurrentPublication(
                "could not update the schedule of " + object.canonicalName());
    }

    private void suspendSchedule(MaterializationId object) {
        if (ports.repository().findSchedule(object)
                .filter(schedule -> schedule.state()
                        == org.pangrova.materialization.domain.ScheduleState.ACTIVE)
                .isEmpty()) {
            return;
        }
        casSchedule(object, schedule -> schedule.state()
                == org.pangrova.materialization.domain.ScheduleState.ACTIVE
                ? schedule.suspend() : schedule);
    }

    private MaterializationOutcome executeAttempt(
            MaterializationObject header,
            DefinitionRevision definition,
            PlannedDefinition planned,
            org.pangrova.materialization.domain.Principal requester,
            java.util.Map<String, String> sessionProperties,
            org.pangrova.materialization.domain.CommandId commandId,
            TriggerKind trigger,
            RefreshResolution resolution) {
        Instant now = ports.clock().now();
        MaterializationId object = header.id();
        AttemptId attemptId = new AttemptId(ports.ids().newId());
        GenerationId generationId = new GenerationId(ports.ids().newId());
        PhysicalTableRef candidate = candidateTable(object);
        SourceVector sourceVector = planned.sourceVector();
        RefreshKind kind = resolution.kind();
        Optional<GenerationId> baseGeneration = resolution.base().map(Generation::id);
        ExecutionFence fence = ports.leases().issueExecutionFence(object, executionLease);
        ports.fences().requireValid(fence, now);

        RefreshAttempt attempt = RefreshAttempt.reserve(
                attemptId, object, header.incarnation(), definition.revision(), header.revision(),
                baseGeneration, kind, sourceVector, trigger, requester,
                definition.definer(), fence, candidate, now);
        Generation generation = Generation.candidate(
                generationId, object, header.incarnation(), definition.revision(), attemptId,
                baseGeneration, candidate, kind, sourceVector, now);
        MaterializationWriteCapability capability =
                ports.authorizer().issueWriteCapability(attempt, generationId);
        List<SourcePin> pins = sourceVector.snapshots().entrySet().stream()
                .map(entry -> SourcePin.acquired(attemptId, entry.getKey(), entry.getValue(),
                        now.plus(sourcePinTtl)))
                .toList();
        ports.repository().reserveAttempt(attempt, generation, pins, capability,
                header.revision(), event(MaterializationEventType.ATTEMPT_RESERVED, object,
                        requester, definition.definer(), commandId, Optional.of(attemptId),
                        Optional.of(generationId), "trigger=" + trigger + ", kind=" + kind, now));

        String pinOwner = attemptId.value().toString();
        List<PinnedSource> held = new ArrayList<>();
        QueryId readerQuery = kind.isIncremental()
                ? new QueryId(attemptId.value().toString()) : null;
        try {
            for (var entry : sourceVector.snapshots().entrySet()) {
                held.add(pinExact(object.tenant().value(), entry.getKey(), entry.getValue(),
                        pinOwner));
            }
            if (resolution.base().isPresent()) {
                Generation base = resolution.base().get();
                for (var entry : base.sourceVector().snapshots().entrySet()) {
                    held.add(pinExact(object.tenant().value(), entry.getKey(), entry.getValue(),
                            pinOwner));
                }
                ports.repository().pinReader(new ReaderPin(base.id(), readerQuery,
                        "materialization-attempt:" + attemptId.value(),
                        now.plus(sourcePinTtl), now));
            }
        } catch (RuntimeException failure) {
            failBeforeExecution(attempt, generation, failure);
            releasePins(held, attemptId, readerQuery, failure);
            throw failure;
        }

        RefreshPlan plan;
        try {
            plan = resolvePlan(definition, resolution, sourceVector, candidate, held);
        } catch (RuntimeException failure) {
            failBeforeExecution(attempt, generation, failure);
            releasePins(held, attemptId, readerQuery, failure);
            throw failure;
        }

        ExecutionResult result;
        try {
            result = ports.execution().execute(new ExecutionRequest(
                    attempt, definition, capability, sessionProperties, Optional.of(plan)));
        } catch (RuntimeException failure) {
            quarantine(attempt, generation, failure);
            releasePins(held, attemptId, readerQuery, failure);
            throw failure;
        }

        Instant committedAt = ports.clock().now();
        ports.repository().recordCommitEvidence(attemptId, result.evidence(), committedAt);
        RefreshAttempt committed = ports.repository().findAttempt(attemptId)
                .orElseThrow(() -> MaterializationException.repositoryFailure(
                        "committed attempt disappeared: " + attemptId.value(), null));
        Generation committedGeneration = ports.repository().findGeneration(generationId)
                .orElseThrow(() -> MaterializationException.repositoryFailure(
                        "committed generation disappeared: " + generationId.value(), null));
        ports.verifiers().verify(
                new PublicationVerification(committedGeneration, committed, result.evidence()));
        PublicationResult published = ports.repository().publish(
                new PublishCommand(object, header.revision(), committedGeneration, committed,
                        result.evidence(), fence, commandId),
                event(MaterializationEventType.GENERATION_PUBLISHED, object, requester,
                        definition.definer(), commandId, Optional.of(attemptId),
                        Optional.of(generationId), "rows=" + result.writtenRows(), committedAt));
        MaterializationOutcome outcome = new MaterializationOutcome(
                published.object(), published.generation(), published.newlyPublished());
        releasePins(held, attemptId, readerQuery, null);
        return outcome;
    }

    /**
     * Builds the executable plan for an attempt that already holds its sources.
     *
     * <p>An incremental attempt pins both endpoints of every source interval,
     * asks the connector to plan and prove the change set, and only then asks
     * the strategy for canonical SQL. Any missing, incomplete, or mismatched
     * proof fails the attempt; it never degrades to a full recomputation.
     */
    private RefreshPlan resolvePlan(DefinitionRevision definition, RefreshResolution resolution,
            SourceVector sourceVector, PhysicalTableRef candidate, List<PinnedSource> held) {
        if (!resolution.kind().isIncremental()) {
            return resolution.strategy().plan(
                    new RefreshContext(definition, Optional.empty(), sourceVector, candidate));
        }
        Generation base = resolution.base().orElseThrow(() -> MaterializationException.incrementalUnsupported(
                "incremental refresh requires a published base generation"));
        Map<SourceRef, ChangeSetPlan> changeSets = new LinkedHashMap<>();
        ChangeProof proof = null;
        for (var entry : sourceVector.snapshots().entrySet()) {
            SourceRef source = entry.getKey();
            SourceSnapshotRef from = base.sourceVector().snapshotOf(source)
                    .orElseThrow(() -> MaterializationException.incrementalUnsupported(
                            "base generation has no snapshot for " + source.canonicalName()));
            if (from.isEmptySource() || entry.getValue().isEmptySource()) {
                throw MaterializationException.incrementalUnsupported(
                        "source " + source.canonicalName()
                                + " was materialized from an empty snapshot and has no append-only interval");
            }
            PinnedSource fromPin = hold(held, source, from.snapshotToken());
            PinnedSource toPin = hold(held, source, entry.getValue().snapshotToken());
            ChangeSetPlan changeSet = planChangeSet(fromPin, toPin);
            if (proof != null) {
                throw MaterializationException.incrementalUnsupported(
                        "append-only refresh supports exactly one source change set");
            }
            changeSets.put(source, changeSet);
            proof = verifyChangeSet(changeSet);
        }
        if (proof == null) {
            throw MaterializationException.incrementalUnsupported(
                    "incremental refresh resolved no source change set");
        }
        RefreshContext context = new RefreshContext(definition, Optional.of(base), sourceVector,
                candidate, changeSets, Optional.of(proof));
        StrategyCompatibility compatibility = resolution.strategy().validate(context);
        if (!compatibility.compatible()) {
            throw MaterializationException.incrementalUnsupported(compatibility.reason());
        }
        RefreshPlan plan = resolution.strategy().plan(context);
        resolution.strategy().verify(plan, proof);
        return plan;
    }

    private ChangeSetPlan planChangeSet(PinnedSource from, PinnedSource to) {
        ChangeSetPlan plan;
        try {
            plan = ports.sourcePinning().changeSet(from, to, ChangeSetMode.APPEND_ONLY);
        } catch (MaterializationSourceException failure) {
            throw MaterializationException.capabilityUnavailable(
                    "cannot plan an append-only change set for " + from.source().canonicalName()
                            + ": " + failure.reason(), failure);
        }
        if (plan == null || !plan.source().equals(from.source())
                || !plan.fromSnapshot().equals(from.snapshot())
                || !plan.toSnapshot().equals(to.snapshot())
                || plan.mode() != ChangeSetMode.APPEND_ONLY) {
            throw MaterializationException.requestRejected(
                    "source provider did not return the planned append-only change set for "
                            + from.source().canonicalName());
        }
        return plan;
    }

    private ChangeProof verifyChangeSet(ChangeSetPlan plan) {
        ChangeProof proof;
        try {
            proof = ports.sourcePinning().verify(plan);
        } catch (MaterializationSourceException failure) {
            throw MaterializationException.capabilityUnavailable(
                    "cannot verify the change set for " + plan.source().canonicalName()
                            + ": " + failure.reason(), failure);
        }
        if (proof == null || !proof.source().equals(plan.source())
                || !proof.fromToken().equals(plan.fromSnapshot().snapshotToken())
                || !proof.toToken().equals(plan.toSnapshot().snapshotToken())
                || proof.mode() != plan.mode()) {
            throw MaterializationException.requestRejected(
                    "source provider did not prove the planned change set for "
                            + plan.source().canonicalName());
        }
        return proof;
    }

    private static PinnedSource hold(List<PinnedSource> held, SourceRef source, String token) {
        return held.stream()
                .filter(pinned -> pinned.source().equals(source)
                        && pinned.snapshot().snapshotToken().equals(token))
                .findFirst()
                .orElseThrow(() -> MaterializationException.requestRejected(
                        "no physical hold exists for " + source.canonicalName() + " at " + token));
    }

    private PinnedSource pinExact(
            String tenant, SourceRef source, SourceSnapshotRef expected, String pinOwner) {
        PinnedSource pinned;
        try {
            pinned = ports.sourcePinning().pin(tenant, source, expected, pinOwner);
        } catch (MaterializationSourceException failure) {
            throw MaterializationException.capabilityUnavailable(
                    "cannot pin source snapshot for " + source.canonicalName()
                            + ": " + failure.reason(), failure);
        }
        if (pinned == null || !tenant.equals(pinned.tenant())
                || !source.equals(pinned.source()) || !expected.equals(pinned.snapshot())
                || !pinOwner.equals(pinned.pinOwner())) {
            MaterializationException mismatch = MaterializationException.requestRejected(
                    "source provider did not pin the exact planned snapshot for "
                            + source.canonicalName());
            if (pinned != null) {
                try {
                    ports.sourcePinning().unpin(pinned);
                } catch (RuntimeException releaseFailure) {
                    mismatch.addSuppressed(releaseFailure);
                }
            }
            throw mismatch;
        }
        return pinned;
    }

    private void releasePins(List<PinnedSource> held, AttemptId attemptId, QueryId readerQuery,
            RuntimeException primary) {
        RuntimeException failure = null;
        if (readerQuery != null) {
            try {
                ports.repository().releaseReaderPins(readerQuery);
            } catch (RuntimeException releaseFailure) {
                failure = releaseFailure;
            }
        }
        for (PinnedSource pinned : held) {
            try {
                ports.sourcePinning().unpin(pinned);
            } catch (RuntimeException releaseFailure) {
                if (failure == null) {
                    failure = releaseFailure;
                } else {
                    failure.addSuppressed(releaseFailure);
                }
            }
        }
        if (failure == null) {
            try {
                ports.repository().releaseSourcePins(attemptId, ports.clock().now());
            } catch (RuntimeException releaseFailure) {
                failure = releaseFailure;
            }
        }
        if (failure != null) {
            if (primary != null) {
                primary.addSuppressed(failure);
            } else {
                throw MaterializationException.repositoryFailure(
                        "source snapshot pin release failed after the attempt settled", failure);
            }
        }
    }

    private void failBeforeExecution(
            RefreshAttempt attempt, Generation generation, RuntimeException failure) {
        try {
            Instant failedAt = ports.clock().now();
            RefreshAttempt failed = attempt.fail(failure.getClass().getSimpleName(), failedAt);
            ports.repository().settleAttempt(failed, generation.collect(),
                    event(MaterializationEventType.ATTEMPT_FAILED, attempt.object(),
                            attempt.requesting(), attempt.definer(), Optional.empty(),
                            Optional.of(attempt.id()), Optional.of(generation.id()),
                            "pre-execution preparation failed", failedAt));
        } catch (RuntimeException settlementFailure) {
            failure.addSuppressed(settlementFailure);
        }
    }

    private void quarantine(RefreshAttempt attempt, Generation generation, RuntimeException failure) {
        try {
            RefreshAttempt building = attempt.markBuilding(ports.clock().now());
            ports.repository().settleAttempt(building, generation,
                    event(MaterializationEventType.ATTEMPT_FAILED, attempt.object(),
                            attempt.requesting(), attempt.definer(), Optional.empty(),
                            Optional.of(attempt.id()), Optional.of(generation.id()),
                            "execution started", ports.clock().now()));
            String reason = failure.getClass().getSimpleName();
            RefreshAttempt quarantined = building.quarantine(reason, ports.clock().now());
            ports.repository().settleAttempt(quarantined, generation.quarantine(),
                    event(MaterializationEventType.ATTEMPT_QUARANTINED, attempt.object(),
                            attempt.requesting(), attempt.definer(), Optional.empty(),
                            Optional.of(attempt.id()), Optional.of(generation.id()),
                            "ambiguous commit outcome: " + reason, ports.clock().now()));
        } catch (RuntimeException settlementFailure) {
            failure.addSuppressed(settlementFailure);
        }
    }

    private PlannedDefinition plan(CreateMaterialization command, long revision) {
        return ports.planner().plan(new PlanningRequest(command.object(), command.kind(),
                command.definitionSql(), command.definer(), revision, command.sessionProperties()));
    }

    private PlannedDefinition planAndValidate(MaterializationId object, MaterializationKind kind,
            DefinitionRevision definition, org.pangrova.materialization.domain.Principal definer,
            java.util.Map<String, String> sessionProperties) {
        PlannedDefinition planned = ports.planner().plan(new PlanningRequest(
                object, kind, definition.sql(), definer, definition.revision(), sessionProperties));
        if (!planned.sources().equals(definition.sources())) {
            throw MaterializationException.invalidDefinition(
                    "definition sources changed; the object must be recreated or altered: "
                            + object.canonicalName());
        }
        if (!planned.outputSchema().equals(definition.outputSchema())) {
            throw MaterializationException.invalidDefinition(
                    "definition output schema changed; the object must be recreated or altered: "
                            + object.canonicalName());
        }
        if (!planned.binding().planFingerprint().equals(definition.binding().planFingerprint())) {
            throw MaterializationException.invalidDefinition(
                    "definition plan changed; the object must be recreated or altered: "
                            + object.canonicalName());
        }
        if (!planned.policySnapshot().fingerprints()
                .equals(definition.policySnapshot().fingerprints())) {
            throw MaterializationException.policyDrift(
                    "source policy changed for " + object.canonicalName());
        }
        return planned;
    }

    private MaterializationOutcome resumeCreate(CreateMaterialization command) {
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        if (header.publishedGeneration().isPresent()) {
            return replay(command.object());
        }
        if (header.state() != ObjectState.Creating.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "cannot resume creation of " + command.object().canonicalName()
                            + " in state " + header.state().stateName());
        }
        DefinitionRevision definition = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        PlannedDefinition planned = planAndValidate(command.object(), header.kind(), definition,
                command.definer(), definition.sessionProperties());
        return executeAttempt(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), TriggerKind.CREATION,
                fullResolution());
    }

    private MaterializationOutcome replay(MaterializationId object) {
        MaterializationObject header = ports.repository().findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        Generation published = ports.repository().findPublishedGeneration(object)
                .orElseThrow(() -> MaterializationException.invalidStateTransition(
                        "replayed command has no published generation: " + object.canonicalName()));
        return new MaterializationOutcome(header, published, false);
    }

    private PhysicalTableRef candidateTable(MaterializationId object) {
        String hex = ports.ids().newId().toString().replace("-", "");
        String catalog = object.name().parts().get(0);
        return new PhysicalTableRef(catalog, InternalNamespace.SCHEMA_NAME,
                InternalNamespace.candidateTableName(hex));
    }

    private MaterializationEvent event(MaterializationEventType type, MaterializationId object,
            org.pangrova.materialization.domain.Principal requesting,
            org.pangrova.materialization.domain.Principal effective,
            org.pangrova.materialization.domain.CommandId command,
            Optional<AttemptId> attempt, Optional<GenerationId> generation,
            String detail, Instant occurredAt) {
        return event(type, object, requesting, effective, Optional.of(command), attempt,
                generation, detail, occurredAt);
    }

    private MaterializationEvent event(MaterializationEventType type, MaterializationId object,
            org.pangrova.materialization.domain.Principal requesting,
            org.pangrova.materialization.domain.Principal effective,
            Optional<org.pangrova.materialization.domain.CommandId> command,
            Optional<AttemptId> attempt, Optional<GenerationId> generation,
            String detail, Instant occurredAt) {
        return new MaterializationEvent(UUID.randomUUID().toString(), type, object, command,
                attempt, generation, Optional.empty(), requesting, effective, detail, occurredAt);
    }

    /**
     * Resolves the kind, base, and strategy of the next refresh.
     *
     * <p>INCREMENTAL never degrades to FULL: a configured strategy must be
     * supported by the planned definition, and AUTO only selects among
     * registered incremental strategies.
     */
    private RefreshResolution resolveRefresh(MaterializationObject header,
            PlannedDefinition planned, Generation published) {
        if (header.refreshMode() == RefreshMode.FULL) {
            return fullResolution();
        }
        RefreshKind kind;
        if (header.incrementalStrategy().isPresent()) {
            StrategyId configured = header.incrementalStrategy().get();
            kind = incrementalKind(configured);
            if (!planned.supportedRefreshKinds().contains(kind)) {
                throw MaterializationException.incrementalUnsupported(
                        "definition of " + header.id().canonicalName()
                                + " does not support configured strategy " + configured.value());
            }
        } else {
            kind = INCREMENTAL_KINDS.stream()
                    .filter(candidate -> planned.supportedRefreshKinds().contains(candidate)
                            && ports.extensions().hasStrategy(candidate.strategyId()))
                    .findFirst()
                    .orElseThrow(() -> MaterializationException.incrementalUnsupported(
                            "no registered incremental strategy applies to "
                                    + header.id().canonicalName()));
        }
        if (!header.refreshMode().accepts(kind)) {
            throw MaterializationException.incrementalUnsupported(
                    "refresh mode " + header.refreshMode() + " does not accept " + kind);
        }
        return new RefreshResolution(kind, Optional.of(published),
                ports.extensions().strategy(kind.strategyId()));
    }

    private RefreshResolution fullResolution() {
        return new RefreshResolution(RefreshKind.Full.INSTANCE, Optional.empty(),
                ports.extensions().strategy(StrategyId.FULL));
    }

    private static RefreshKind incrementalKind(StrategyId strategy) {
        if (StrategyId.FULL.equals(strategy)) {
            throw MaterializationException.requestRejected(
                    "FULL is not an incremental strategy");
        }
        return INCREMENTAL_KINDS.stream()
                .filter(kind -> kind.strategyId().equals(strategy))
                .findFirst()
                .orElseThrow(() -> MaterializationException.incrementalUnsupported(
                        "unknown incremental strategy " + strategy.value()));
    }

    /**
     * Validates the configured refresh mode against the definition at creation.
     *
     * <p>Creation always bootstraps a full generation, but an object configured
     * for incremental refresh must be able to resolve an incremental strategy
     * now; otherwise the configuration could never take effect.
     */
    private void requireRefreshConfiguration(RefreshMode mode, Optional<StrategyId> strategy,
            Set<RefreshKind> supportedRefreshKinds) {
        if (mode == RefreshMode.FULL) {
            if (strategy.isPresent()) {
                throw MaterializationException.requestRejected(
                        "FULL refresh mode cannot carry an incremental strategy");
            }
            return;
        }
        if (strategy.isPresent()) {
            RefreshKind kind = incrementalKind(strategy.get());
            if (!supportedRefreshKinds.contains(kind)) {
                throw MaterializationException.incrementalUnsupported(
                        "definition does not support configured strategy "
                                + strategy.get().value());
            }
            ports.extensions().strategy(strategy.get());
            return;
        }
        boolean available = INCREMENTAL_KINDS.stream()
                .anyMatch(kind -> supportedRefreshKinds.contains(kind)
                        && ports.extensions().hasStrategy(kind.strategyId()));
        if (!available) {
            throw MaterializationException.incrementalUnsupported(
                    "definition has no compatible incremental strategy; "
                            + "create it with full refresh instead");
        }
    }

    /** Resolved execution decision for one attempt. */
    private record RefreshResolution(RefreshKind kind, Optional<Generation> base,
            RefreshStrategy strategy) {
        RefreshResolution {
            if (kind == null || base == null || strategy == null) {
                throw new IllegalArgumentException("refresh resolution must be complete");
            }
            if (kind.isIncremental() != base.isPresent()) {
                throw new IllegalArgumentException(
                        "only incremental refreshes resolve a base generation");
            }
        }
    }

    private static final List<RefreshKind> INCREMENTAL_KINDS = List.of(
            RefreshKind.IncrementalAppendOnly.INSTANCE,
            RefreshKind.IncrementalPartitionReplace.INSTANCE,
            RefreshKind.IncrementalAffectedKey.INSTANCE,
            RefreshKind.IncrementalCdc.INSTANCE);

    /** Only definer security is part of the kernel contract. */
    private static final class SecurityModeGuard {
        private SecurityModeGuard() {}

        static void require(org.pangrova.materialization.domain.SecurityMode mode) {
            if (!mode.isSupported()) {
                throw MaterializationException.requestRejected(
                        "unsupported materialization security mode: " + mode);
            }
        }
    }
}
