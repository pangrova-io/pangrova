package org.pangrova.materialization.usecase;

import java.time.Instant;
import java.util.Optional;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.IncarnationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.ObjectState;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.SecurityMode;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.MaterializationAction;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PlanningRequest;

/**
 * Creation and replacement commands of the materialization kernel.
 *
 * <p>Both commands follow the same protocol: plan and pin sources, reserve an
 * immutable candidate under a live execution fence, execute in the data plane,
 * record exact commit evidence, verify it, and publish with CAS. A failure
 * whose commit outcome cannot be disproved quarantines the candidate instead
 * of pretending the attempt failed cleanly.
 */
final class MaterializationCreationCommands {

    private final KernelPorts ports;
    private final MaterializationAttemptExecutor attemptExecutor;
    private final RefreshResolutionPolicy resolutionPolicy;

    MaterializationCreationCommands(KernelPorts ports,
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

    /** Creates an object and publishes its first generation. */
    MaterializationOutcome create(CreateMaterialization command) {
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            MaterializationCommandSupport.requireReceiptScope(receipt.orElseThrow(), command.object());
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
                command.object(), command.kind(), new IncarnationId(
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
    MaterializationOutcome createOrReplace(CreateMaterialization command) {
        if (command.ifNotExists()) {
            throw MaterializationException.requestRejected(
                    "OR REPLACE and IF NOT EXISTS are mutually exclusive");
        }
        Optional<CommandRecord> receipt = ports.repository().findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent()) {
            MaterializationCommandSupport.requireReceiptScope(receipt.orElseThrow(), command.object());
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
        command.targetLag().ifPresent(lag -> MaterializationCommandSupport.casSchedule(ports,
                command.object(), schedule -> schedule.changeTargetLag(lag, now)));
        return attemptExecutor.execute(replacement, definition, planned, command.requester(),
                command.sessionProperties(), command.commandId(), TriggerKind.REPAIR,
                resolutionPolicy.full());
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
        PlannedDefinition planned = MaterializationCommandSupport.planAndValidate(ports,
                command.object(), header.kind(), definition,
                definition.definer(), definition.definerGroups(), definition.sessionProperties());
        ports.authorizer().requireManage(command.object(), command.requester(),
                MaterializationAction.ALTER);
        ports.authorizer().requireCreate(
                command.object(), header.kind(), command.requester(), planned.sources());
        return attemptExecutor.execute(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), TriggerKind.REPAIR,
                resolutionPolicy.full());
    }

    private MaterializationOutcome resumeCreate(CreateMaterialization command) {
        MaterializationObject header = ports.repository().findObject(command.object())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        if (header.publishedGeneration().isPresent()) {
            return MaterializationCommandSupport.replay(ports, command.object());
        }
        if (header.state() != ObjectState.Creating.INSTANCE) {
            throw MaterializationException.invalidStateTransition(
                    "cannot resume creation of " + command.object().canonicalName()
                            + " in state " + header.state().stateName());
        }
        DefinitionRevision definition = ports.repository()
                .findDefinition(command.object(), header.definitionRevision())
                .orElseThrow(() -> MaterializationException.notFound(command.object()));
        PlannedDefinition planned = MaterializationCommandSupport.planAndValidate(ports,
                command.object(), header.kind(), definition,
                definition.definer(), definition.definerGroups(), definition.sessionProperties());
        ports.authorizer().requireCreate(
                command.object(), header.kind(), command.requester(), planned.sources());
        return attemptExecutor.execute(header, definition, planned, command.requester(),
                definition.sessionProperties(), command.commandId(), TriggerKind.CREATION,
                resolutionPolicy.full());
    }

    private PlannedDefinition plan(CreateMaterialization command, long revision) {
        return ports.planner().plan(new PlanningRequest(command.object(), command.kind(),
                command.definitionSql(), command.definer(), command.definerGroups(), revision,
                command.sessionProperties()));
    }

    /** Only definer security is part of the kernel contract. */
    private static final class SecurityModeGuard {
        private SecurityModeGuard() {}

        static void require(SecurityMode mode) {
            if (!mode.isSupported()) {
                throw MaterializationException.requestRejected(
                        "unsupported materialization security mode: " + mode);
            }
        }
    }
}
