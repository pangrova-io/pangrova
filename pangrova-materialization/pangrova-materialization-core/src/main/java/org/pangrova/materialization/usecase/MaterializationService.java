package org.pangrova.materialization.usecase;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.pangrova.connector.api.materialization.SourceRef;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationKind;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.MaterializationAction;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;

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
    private final MaterializationAttemptExecutor attemptExecutor;
    private final RefreshResolutionPolicy resolutionPolicy;

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
        this.attemptExecutor = new MaterializationAttemptExecutor(ports, executionLease,
                sourcePinTtl);
        this.resolutionPolicy = new RefreshResolutionPolicy(ports.extensions());
    }

    /** Creates an object and publishes its first generation. */
    public MaterializationOutcome create(CreateMaterialization command) {
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            requireReceiptScope(receipt.orElseThrow(), command.object());
            return resumeCreate(command);
        }
        Optional<MaterializationObject> existing = ports.repository().findObject(command.object());
        MaterializationObject dropped = null;
        if (existing.isPresent()) {
            if (existing.get().state() == ObjectState.Dropped.INSTANCE) {
                // A dropped name is free again; the new incarnation keeps the
                // durable audit trail of the previous one.
                dropped = existing.get();
            } else if (command.ifNotExists()) {
                return existingOutcome(command, existing.get());
            } else {
                throw MaterializationException.requestRejected(
                        "materialization " + command.object().canonicalName() + " already exists");
            }
        }
        SecurityModeGuard.require(command.securityMode());
        Instant now = ports.clock().now();
        PlannedDefinition planned = plan(command, 1);
        resolutionPolicy.requireConfiguration(command.refreshMode(), command.incrementalStrategy(),
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
                command.definerGroups(), command.sessionProperties(), now);
        Schedule schedule = command.targetLag()
                .map(lag -> Schedule.scheduled(command.object(), lag, lag.deadlineFrom(now)))
                .orElseGet(() -> Schedule.manual(command.object()));
        boolean created = dropped == null
                ? ports.repository().createObjectIfAbsent(header, definition, schedule,
                        command.commandId(),
                        MaterializationEvents.of(MaterializationEventType.OBJECT_CREATED, command.object(),
                                command.requester(), command.definer(), command.commandId(),
                                Optional.empty(), Optional.empty(),
                                "created " + command.kind(), now))
                : ports.repository().reclaimDroppedObject(dropped, header, definition, schedule,
                        command.commandId(),
                        MaterializationEvents.of(MaterializationEventType.OBJECT_CREATED, command.object(),
                                command.requester(), command.definer(), command.commandId(),
                                Optional.empty(), Optional.empty(),
                                "recreated " + command.kind(), now));
        if (!created) {
            MaterializationObject conflicting = ports.repository().findObject(command.object())
                    .orElseThrow(() -> MaterializationException.repositoryFailure(
                            "a conflicting create disappeared for "
                                    + command.object().canonicalName(), null));
            if (command.ifNotExists() && conflicting.state() != ObjectState.Dropped.INSTANCE) {
                return existingOutcome(command, conflicting);
            }
            throw MaterializationException.requestRejected(
                    "materialization " + command.object().canonicalName() + " already exists");
        }

        return attemptExecutor.execute(header, definition, planned, command.requester(),
                command.sessionProperties(), command.commandId(), TriggerKind.CREATION,
                resolutionPolicy.full());
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
            requireReceiptScope(receipt.orElseThrow(), command.object());
            return resumeReplace(command);
        }
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElse(null);
        if (header == null) {
            return create(command);
        }
        if (header.state() == ObjectState.Dropped.INSTANCE) {
            // Replacing a dropped name is a fresh incarnation, not a redefine.
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
        resolutionPolicy.requireConfiguration(command.refreshMode(), command.incrementalStrategy(),
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
                command.definerGroups(), command.sessionProperties(), now);
        MaterializationObject replacement = header.withReplacement(nextRevision,
                command.refreshMode(), command.incrementalStrategy(), command.definer(),
                header.revision(), now);
        ports.repository().replaceDefinition(definition, replacement, header.revision(),
                command.commandId(),
                MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, command.object(),
                        command.requester(), header.definer(), command.commandId(),
                        Optional.empty(), Optional.empty(),
                        "replaced definition at revision " + nextRevision, now));
        command.targetLag().ifPresent(lag -> casSchedule(command.object(),
                schedule -> schedule.changeTargetLag(lag, now)));
        return attemptExecutor.execute(replacement, definition, planned, command.requester(),
                command.sessionProperties(), command.commandId(), TriggerKind.REPAIR,
                resolutionPolicy.full());
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
            ports.authorizer().requireManage(command.object(), command.requester(),
                    MaterializationAction.ALTER);
            return replay(command.object());
        }
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            requireReceiptScope(receipt.orElseThrow(), command.object());
            ports.authorizer().requireManage(command.object(), command.requester(),
                    MaterializationAction.ALTER);
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
                header.definer(), definition.definerGroups(), definition.sessionProperties());
        resolutionPolicy.requireConfiguration(command.mode(), strategy,
                planned.supportedRefreshKinds());
        MaterializationOutcome rebuilt = attemptExecutor.execute(header, definition, planned,
                command.requester(), definition.sessionProperties(), command.commandId(),
                TriggerKind.REPAIR, resolutionPolicy.full());
        return applyRefreshConfiguration(command, strategy, rebuilt.object(), true);
    }

    private MaterializationOutcome applyRefreshConfiguration(SetRefreshMode command,
            Optional<StrategyId> strategy, MaterializationObject header, boolean rebuilt) {
        Instant now = ports.clock().now();
        MaterializationObject configured = header.withRefreshConfiguration(
                command.mode(), strategy, header.revision(), now);
        ports.repository().updateObject(configured, header.revision(), command.commandId(),
                MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, command.object(),
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
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            requireReceiptScope(receipt.orElseThrow(), command.newObject());
            ports.authorizer().requireManage(command.object(), command.requester(),
                    MaterializationAction.ALTER);
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
                MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, command.newObject(),
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
            ports.authorizer().requireManage(command.object(), command.requester(),
                    MaterializationAction.ALTER);
            return new MaterializationOutcome(header, published.get(), false);
        }
        DefinitionRevision definition = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        PlannedDefinition planned = planAndValidate(command.object(), header.kind(), definition,
                definition.definer(), definition.definerGroups(), definition.sessionProperties());
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.ALTER);
        ports.authorizer().requireCreate(
                command.object(), header.kind(), command.requester(), planned.sources());
        return attemptExecutor.execute(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), TriggerKind.REPAIR,
                resolutionPolicy.full());
    }

    /** Recomputes an active object and publishes the new generation. */
    public MaterializationOutcome refresh(RefreshMaterialization command) {
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            requireReceiptScope(receipt.orElseThrow(), command.object());
            if (command.trigger() == TriggerKind.MANUAL) {
                ports.authorizer().requireManage(command.object(), command.requester(),
                        MaterializationAction.REFRESH);
            }
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
        if (command.trigger() == TriggerKind.MANUAL) {
            ports.authorizer().requireManage(command.object(), command.requester(),
                    MaterializationAction.REFRESH);
        }

        PlannedDefinition planned = planAndValidate(command.object(), header.kind(), definition,
                header.definer(), definition.definerGroups(), definition.sessionProperties());
        RefreshResolution resolution = resolutionPolicy.resolve(header, planned, published);
        return attemptExecutor.execute(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), command.trigger(), resolution);
    }

    /** Records a new definition revision; the next refresh publishes it. */
    public void redefine(RedefineMaterialization command) {
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            requireReceiptScope(receipt.orElseThrow(), command.object());
            ports.authorizer().requireManage(command.object(), command.requester(),
                    MaterializationAction.ALTER);
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
        DefinitionRevision current = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        PlannedDefinition planned = ports.planner().plan(new PlanningRequest(
                command.object(), header.kind(), command.definitionSql(), header.definer(),
                current.definerGroups(), nextRevision, command.sessionProperties()));
        Instant now = ports.clock().now();
        DefinitionRevision definition = new DefinitionRevision(
                command.object(), nextRevision, command.definitionSql(), planned.binding(),
                planned.outputSchema(), planned.sources(), planned.dependencies(),
                planned.policySnapshot(), header.securityMode(), header.definer(),
                current.definerGroups(), command.sessionProperties(), now);
        MaterializationObject redefined =
                header.withDefinitionRevision(nextRevision, header.revision(), now);
        ports.repository().replaceDefinition(definition, redefined, header.revision(),
                command.commandId(),
                MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, command.object(),
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
                    MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, object,
                            command.requester(), header.definer(), command.commandId(),
                            Optional.empty(), Optional.empty(), "dropping", now));
        }
        MaterializationObject dropped = dropping.dropped(dropping.revision(), now);
        ports.repository().dropObject(dropped, dropping.publishedGeneration(),
                dropping.revision(), command.commandId(),
                MaterializationEvents.of(MaterializationEventType.OBJECT_DROPPED, object,
                        command.requester(), header.definer(), command.commandId(),
                        Optional.empty(), Optional.empty(), "dropped", now));
    }

    /** Suspends automatic refresh; already suspended objects are unchanged. */
    public void suspend(SuspendMaterialization command) {
        MaterializationObject header = requireAlterable(command.object(), command.requester());
        Schedule current = ports.repository().findSchedule(command.object())
                .orElseThrow(() -> MaterializationException.requestRejected(
                        "materialization " + command.object().canonicalName()
                                + " has no schedule"));
        // A manual-only object has nothing to suspend, and pausing its header
        // would strand it because resume requires a target lag.
        if (current.state() == org.pangrova.materialization.domain.ScheduleState.DISABLED) {
            return;
        }
        if (header.state() == ObjectState.Active.INSTANCE) {
            Instant now = ports.clock().now();
            MaterializationObject suspended = header.transitionTo(
                    ObjectState.Suspended.INSTANCE, header.revision(), now);
            ports.repository().updateObject(suspended, header.revision(), command.commandId(),
                    MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, command.object(),
                            command.requester(), header.definer(), command.commandId(),
                            Optional.empty(), Optional.empty(), "suspended", now));
        }
        casSchedule(command.object(), schedule -> schedule.state()
                == org.pangrova.materialization.domain.ScheduleState.ACTIVE
                ? schedule.suspend() : schedule);
    }

    /** Resumes automatic refresh; already active objects are unchanged. */
    public void resume(ResumeMaterialization command) {
        MaterializationObject header = requireAlterable(command.object(), command.requester());
        casSchedule(command.object(), schedule -> switch (schedule.state()) {
            case ACTIVE -> schedule;
            case SUSPENDED -> schedule.resume(ports.clock().now());
            case DISABLED -> throw MaterializationException.invalidStateTransition(
                    "materialization " + command.object().canonicalName()
                            + " has no target lag to resume");
        });
        if (header.state() == ObjectState.Suspended.INSTANCE) {
            Instant now = ports.clock().now();
            MaterializationObject active = header.transitionTo(
                    ObjectState.Active.INSTANCE, header.revision(), now);
            ports.repository().updateObject(active, header.revision(), command.commandId(),
                    MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, command.object(),
                            command.requester(), header.definer(), command.commandId(),
                            Optional.empty(), Optional.empty(), "resumed", now));
        }
    }

    /** Changes the freshness target, activating a manual schedule. */
    public void setTargetLag(SetTargetLag command) {
        requireAlterable(command.object(), command.requester());
        casSchedule(command.object(),
                schedule -> schedule.changeTargetLag(command.targetLag(), ports.clock().now()));
    }

    private MaterializationObject requireAlterable(
            MaterializationId object, Principal requester) {
        MaterializationObject header = ports.repository().findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        if (header.state() == ObjectState.Dropping.INSTANCE
                || header.state() == ObjectState.Dropped.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "materialization " + object.canonicalName() + " is " + header.state().stateName());
        }
        ports.authorizer().requireManage(object, requester, MaterializationAction.ALTER);
        return header;
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

    private PlannedDefinition plan(CreateMaterialization command, long revision) {
        return ports.planner().plan(new PlanningRequest(command.object(), command.kind(),
                command.definitionSql(), command.definer(), command.definerGroups(), revision,
                command.sessionProperties()));
    }

    private PlannedDefinition planAndValidate(MaterializationId object, MaterializationKind kind,
            DefinitionRevision definition, org.pangrova.materialization.domain.Principal definer,
            java.util.Set<String> definerGroups,
            java.util.Map<String, String> sessionProperties) {
        PlannedDefinition planned = ports.planner().plan(new PlanningRequest(
                object, kind, definition.sql(), definer, definerGroups, definition.revision(),
                sessionProperties));
        boolean generatedSources = hasGeneratedSources(planned.sources())
                || hasGeneratedSources(definition.sources());
        if (!declaredSources(planned.sources()).equals(declaredSources(definition.sources()))) {
            throw MaterializationException.invalidDefinition(
                    "definition sources changed; the object must be recreated or altered: "
                            + object.canonicalName());
        }
        if (!planned.outputSchema().equals(definition.outputSchema())) {
            throw MaterializationException.invalidDefinition(
                    "definition output schema changed; the object must be recreated or altered: "
                            + object.canonicalName());
        }
        if (!planned.dependencies().equals(definition.dependencies())) {
            throw MaterializationException.invalidDefinition(
                    "definition dependencies changed; the object must be recreated or altered: "
                            + object.canonicalName());
        }
        // An upstream generation table is replaced on every publication, so the
        // physical plan necessarily changes when a definition reads another
        // materialization. The declared sources, output schema, and dependency
        // edges above are the stable semantics; only definitions without
        // generated sources bind a plan and policy snapshot for their lifetime.
        if (!generatedSources) {
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
        }
        return planned;
    }

    private static boolean hasGeneratedSources(Set<SourceRef> sources) {
        return sources.stream()
                .anyMatch(source -> InternalNamespace.SCHEMA_NAME.equals(source.schema()));
    }

    private static Set<SourceRef> declaredSources(Set<SourceRef> sources) {
        return sources.stream()
                .filter(source -> !InternalNamespace.SCHEMA_NAME.equals(source.schema()))
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
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
                definition.definer(), definition.definerGroups(), definition.sessionProperties());
        ports.authorizer().requireCreate(
                command.object(), header.kind(), command.requester(), planned.sources());
        return attemptExecutor.execute(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), TriggerKind.CREATION,
                resolutionPolicy.full());
    }

    private MaterializationOutcome replay(MaterializationId object) {
        MaterializationObject header = ports.repository().findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object));
        Generation published = ports.repository().findPublishedGeneration(object)
                .orElseThrow(() -> MaterializationException.invalidStateTransition(
                        "replayed command has no published generation: " + object.canonicalName()));
        return new MaterializationOutcome(header, published, false);
    }

    /** Checks SEL/read authority on one materialization. */
    public void requireRead(MaterializationId object, org.pangrova.materialization.domain.Principal requester) {
        ports.authorizer().requireRead(object, requester);
    }

    private static void requireReceiptScope(CommandRecord receipt, MaterializationId object) {
        String marker = "\"object\":\"" + object.canonicalName() + "\"";
        if (!receipt.resultReference().contains(marker)) {
            throw MaterializationException.requestRejected(
                    "command id " + receipt.commandId().value()
                            + " was already used for another materialization");
        }
    }

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
