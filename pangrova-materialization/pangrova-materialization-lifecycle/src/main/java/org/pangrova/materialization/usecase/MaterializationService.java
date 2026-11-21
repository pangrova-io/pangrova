package org.pangrova.materialization.usecase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.enums.ObjectState;
import org.pangrova.materialization.api.domain.Principal;
import org.pangrova.materialization.api.enums.RefreshMode;
import org.pangrova.materialization.api.domain.Schedule;
import org.pangrova.materialization.api.identity.StrategyId;
import org.pangrova.materialization.api.enums.TriggerKind;
import org.pangrova.materialization.api.port.CommandRecord;
import org.pangrova.materialization.api.enums.MaterializationAction;
import org.pangrova.materialization.api.enums.MaterializationEventType;
import org.pangrova.materialization.api.port.PlannedDefinition;
import org.pangrova.materialization.api.port.PlanningRequest;

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
    private final MaterializationSchedules schedules;
    private final MaterializationRemoval removal;
    private final MaterializationDefinitionPlanning definitions;
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
        this.schedules = new MaterializationSchedules(ports.repository());
        this.removal = new MaterializationRemoval(ports.repository(), ports.authorizer(), ports.clock(), schedules);
        this.definitions = new MaterializationDefinitionPlanning(ports.planner());
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
            if (existing.get().state() == ObjectState.DROPPED) {
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
        PlannedDefinition planned = definitions.plan(command, 1);
        resolutionPolicy.requireConfiguration(command.refreshMode(), command.incrementalStrategy(),
                planned.supportedRefreshKinds());
        ports.authorizer().requireCreate(
                command.object(), command.kind(), command.requester(), planned.sources());

        MaterializationObject header = MaterializationObject.creating(
                command.object(), command.kind(), new org.pangrova.materialization.api.domain.IncarnationId(
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
            if (command.ifNotExists() && conflicting.state() != ObjectState.DROPPED) {
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
        if (header.state() == ObjectState.DROPPED) {
            // Replacing a dropped name is a fresh incarnation, not a redefine.
            return create(command);
        }
        if (header.kind() != command.kind()) {
            throw MaterializationException.requestRejected(
                    "materialization " + command.object().canonicalName()
                            + " already exists as " + header.kind());
        }
        if (header.state() != ObjectState.ACTIVE
                && header.state() != ObjectState.SUSPENDED) {
            throw MaterializationException.invalidStateTransition(
                    "replace requires an active or suspended materialization, found "
                            + header.state().stateName());
        }
        SecurityModeGuard.require(command.securityMode());
        long nextRevision = header.definitionRevision() + 1;
        PlannedDefinition planned = definitions.plan(command, nextRevision);
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
        command.targetLag().ifPresent(lag -> schedules.update(command.object(),
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
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
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
        if (header.state() != ObjectState.ACTIVE) {
            throw MaterializationException.invalidStateTransition(
                    "refresh mode change requires an active materialization, found "
                            + header.state().stateName());
        }
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.ALTER);
        DefinitionRevision definition = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
        PlannedDefinition planned = definitions.planAndValidate(command.object(), header.kind(), definition,
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
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
        if (header.state() != ObjectState.ACTIVE
                && header.state() != ObjectState.SUSPENDED) {
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
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
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
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
        PlannedDefinition planned = definitions.planAndValidate(command.object(), header.kind(), definition,
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
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
        if (header.state() != ObjectState.ACTIVE) {
            throw MaterializationException.invalidStateTransition(
                    "refresh requires an active materialization, found " + header.state().stateName());
        }
        Generation published = ports.repository().findPublishedGeneration(command.object())
                .orElseThrow(() -> MaterializationException.invalidStateTransition(
                        "active object has no published generation: " + command.object().canonicalName()));
        DefinitionRevision definition = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
        if (command.trigger() == TriggerKind.MANUAL) {
            ports.authorizer().requireManage(command.object(), command.requester(),
                    MaterializationAction.REFRESH);
        }

        PlannedDefinition planned = definitions.planAndValidate(command.object(), header.kind(), definition,
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
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
        if (header.state() != ObjectState.ACTIVE
                && header.state() != ObjectState.SUSPENDED) {
            throw MaterializationException.invalidStateTransition(
                    "redefine requires an active or suspended materialization, found "
                            + header.state().stateName());
        }
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.ALTER);
        long nextRevision = header.definitionRevision() + 1;
        DefinitionRevision current = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
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

    /** Releases the logical name after checking authority over every affected dependent. */
    public void drop(DropMaterialization command) {
        removal.drop(command);
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
        if (current.state() == org.pangrova.materialization.api.enums.ScheduleState.DISABLED) {
            return;
        }
        if (header.state() == ObjectState.ACTIVE) {
            Instant now = ports.clock().now();
            MaterializationObject suspended = header.transitionTo(
                    ObjectState.SUSPENDED, header.revision(), now);
            ports.repository().updateObject(suspended, header.revision(), command.commandId(),
                    MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, command.object(),
                            command.requester(), header.definer(), command.commandId(),
                            Optional.empty(), Optional.empty(), "suspended", now));
        }
        schedules.update(command.object(), schedule -> schedule.state()
                == org.pangrova.materialization.api.enums.ScheduleState.ACTIVE
                ? schedule.suspend() : schedule);
    }

    /** Resumes automatic refresh; already active objects are unchanged. */
    public void resume(ResumeMaterialization command) {
        MaterializationObject header = requireAlterable(command.object(), command.requester());
        schedules.update(command.object(), schedule -> switch (schedule.state()) {
            case ACTIVE -> schedule;
            case SUSPENDED -> schedule.resume(ports.clock().now());
            case DISABLED -> throw MaterializationException.invalidStateTransition(
                    "materialization " + command.object().canonicalName()
                            + " has no target lag to resume");
        });
        if (header.state() == ObjectState.SUSPENDED) {
            Instant now = ports.clock().now();
            MaterializationObject active = header.transitionTo(
                    ObjectState.ACTIVE, header.revision(), now);
            ports.repository().updateObject(active, header.revision(), command.commandId(),
                    MaterializationEvents.of(MaterializationEventType.OBJECT_ALTERED, command.object(),
                            command.requester(), header.definer(), command.commandId(),
                            Optional.empty(), Optional.empty(), "resumed", now));
        }
    }

    /** Changes the freshness target, activating a manual schedule. */
    public void setTargetLag(SetTargetLag command) {
        requireAlterable(command.object(), command.requester());
        schedules.update(command.object(),
                schedule -> schedule.changeTargetLag(command.targetLag(), ports.clock().now()));
    }

    private MaterializationObject requireAlterable(
            MaterializationId object, Principal requester) {
        MaterializationObject header = ports.repository().findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object.canonicalName()));
        if (header.state() == ObjectState.DROPPING
                || header.state() == ObjectState.DROPPED) {
            throw MaterializationException.invalidStateTransition(
                    "materialization " + object.canonicalName() + " is " + header.state().stateName());
        }
        ports.authorizer().requireManage(object, requester, MaterializationAction.ALTER);
        return header;
    }

    private MaterializationOutcome resumeCreate(CreateMaterialization command) {
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
        if (header.publishedGeneration().isPresent()) {
            return replay(command.object());
        }
        if (header.state() != ObjectState.CREATING) {
            throw MaterializationException.invalidStateTransition(
                    "cannot resume creation of " + command.object().canonicalName()
                            + " in state " + header.state().stateName());
        }
        DefinitionRevision definition = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object().canonicalName()));
        PlannedDefinition planned = definitions.planAndValidate(command.object(), header.kind(), definition,
                definition.definer(), definition.definerGroups(), definition.sessionProperties());
        ports.authorizer().requireCreate(
                command.object(), header.kind(), command.requester(), planned.sources());
        return attemptExecutor.execute(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), TriggerKind.CREATION,
                resolutionPolicy.full());
    }

    private MaterializationOutcome replay(MaterializationId object) {
        MaterializationObject header = ports.repository().findObject(object)
                .orElseThrow(() -> MaterializationException.notFound(object.canonicalName()));
        Generation published = ports.repository().findPublishedGeneration(object)
                .orElseThrow(() -> MaterializationException.invalidStateTransition(
                        "replayed command has no published generation: " + object.canonicalName()));
        return new MaterializationOutcome(header, published, false);
    }

    /** Checks SEL/read authority on one materialization. */
    public void requireRead(MaterializationId object, org.pangrova.materialization.api.domain.Principal requester) {
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

        static void require(org.pangrova.materialization.api.enums.SecurityMode mode) {
            if (!mode.isSupported()) {
                throw MaterializationException.requestRejected(
                        "unsupported materialization security mode: " + mode);
            }
        }
    }
}
