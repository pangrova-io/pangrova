package org.pangrova.materialization.usecase;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.RefreshMode;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.ScheduleState;
import org.pangrova.materialization.domain.StrategyId;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.MaterializationAction;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;

/**
 * Configuration and lifecycle commands of the materialization kernel: refresh
 * mode changes, renames, redefinitions, refresh, drop, and schedule control.
 */
final class MaterializationLifecycleCommands {

    private final KernelPorts ports;
    private final MaterializationAttemptExecutor attemptExecutor;
    private final RefreshResolutionPolicy resolutionPolicy;

    MaterializationLifecycleCommands(KernelPorts ports,
            MaterializationAttemptExecutor attemptExecutor,
            RefreshResolutionPolicy resolutionPolicy) {
        if (ports == null || attemptExecutor == null || resolutionPolicy == null) {
            throw new IllegalArgumentException(
                    "ports, attemptExecutor, and resolutionPolicy must not be null");
        }
        this.ports = ports;
        this.attemptExecutor = attemptExecutor;
        this.resolutionPolicy = resolutionPolicy;
    }

    /**
     * Switches refresh mode and rebuilds the published head with a full refresh.
     *
     * <p>The configuration is committed only after the forced full generation
     * publishes, so a failed rebuild leaves the previous mode and head intact.
     * A retry after a crash between publication and configuration update applies
     * the remaining configuration change without recomputing data.
     */
    MaterializationOutcome setRefreshMode(SetRefreshMode command) {
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        Optional<StrategyId> strategy = command.mode() == RefreshMode.INCREMENTAL
                ? command.strategy().or(() -> header.incrementalStrategy())
                : Optional.empty();
        if (header.refreshMode() == command.mode()
                && header.incrementalStrategy().equals(strategy)) {
            ports.authorizer().requireManage(command.object(), command.requester(),
                    MaterializationAction.ALTER);
            return MaterializationCommandSupport.replay(ports, command.object());
        }
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            MaterializationCommandSupport.requireReceiptScope(receipt.orElseThrow(), command.object());
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
        PlannedDefinition planned = MaterializationCommandSupport.planAndValidate(ports,
                command.object(), header.kind(), definition,
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
    void rename(RenameMaterialization command) {
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            MaterializationCommandSupport.requireReceiptScope(receipt.orElseThrow(), command.newObject());
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
                                    .collect(Collectors.joining(", "))
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

    /** Recomputes an active object and publishes the new generation. */
    MaterializationOutcome refresh(RefreshMaterialization command) {
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            MaterializationCommandSupport.requireReceiptScope(receipt.orElseThrow(), command.object());
            if (command.trigger() == TriggerKind.MANUAL) {
                ports.authorizer().requireManage(command.object(), command.requester(),
                        MaterializationAction.REFRESH);
            }
            return MaterializationCommandSupport.replay(ports, command.object());
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

        PlannedDefinition planned = MaterializationCommandSupport.planAndValidate(ports,
                command.object(), header.kind(), definition,
                header.definer(), definition.definerGroups(), definition.sessionProperties());
        RefreshResolution resolution = resolutionPolicy.resolve(header, planned, published);
        return attemptExecutor.execute(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), command.trigger(), resolution);
    }

    /** Records a new definition revision; the next refresh publishes it. */
    void redefine(RedefineMaterialization command) {
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            MaterializationCommandSupport.requireReceiptScope(receipt.orElseThrow(), command.object());
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
    void drop(DropMaterialization command) {
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
        collectDependentsFirst(command.object(), new LinkedHashSet<>(), order);
        if (order.size() > 1 && !command.cascade()) {
            order.remove(command.object());
            throw MaterializationException.dependencyConflict(
                    "materialization " + command.object().canonicalName() + " is referenced by "
                            + order.stream().map(MaterializationId::canonicalName)
                                    .collect(Collectors.joining(", "))
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
        MaterializationCommandSupport.suspendSchedule(ports, object);
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
    void suspend(SuspendMaterialization command) {
        MaterializationObject header = requireAlterable(command.object(), command.requester());
        Schedule current = ports.repository().findSchedule(command.object())
                .orElseThrow(() -> MaterializationException.requestRejected(
                        "materialization " + command.object().canonicalName()
                                + " has no schedule"));
        // A manual-only object has nothing to suspend, and pausing its header
        // would strand it because resume requires a target lag.
        if (current.state() == ScheduleState.DISABLED) {
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
        MaterializationCommandSupport.casSchedule(ports, command.object(),
                schedule -> schedule.state() == ScheduleState.ACTIVE
                        ? schedule.suspend() : schedule);
    }

    /** Resumes automatic refresh; already active objects are unchanged. */
    void resume(ResumeMaterialization command) {
        MaterializationObject header = requireAlterable(command.object(), command.requester());
        MaterializationCommandSupport.casSchedule(ports, command.object(),
                schedule -> switch (schedule.state()) {
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
    void setTargetLag(SetTargetLag command) {
        requireAlterable(command.object(), command.requester());
        MaterializationCommandSupport.casSchedule(ports, command.object(),
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
}
