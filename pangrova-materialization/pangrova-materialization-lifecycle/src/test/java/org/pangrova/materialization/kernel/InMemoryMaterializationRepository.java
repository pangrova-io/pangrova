package org.pangrova.materialization.kernel;

import org.pangrova.materialization.api.enums.GcState;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.pangrova.materialization.api.domain.AttemptId;
import org.pangrova.materialization.api.enums.AttemptState;
import org.pangrova.materialization.api.domain.CommandId;
import org.pangrova.materialization.api.domain.DefinitionRevision;
import org.pangrova.materialization.api.domain.Dependency;
import org.pangrova.materialization.api.domain.GcLedgerEntry;
import org.pangrova.materialization.api.domain.Generation;
import org.pangrova.materialization.api.domain.GenerationId;
import org.pangrova.materialization.api.enums.GenerationState;
import org.pangrova.materialization.api.exception.MaterializationException;
import org.pangrova.materialization.api.domain.MaterializationId;
import org.pangrova.materialization.api.domain.MaterializationObject;
import org.pangrova.materialization.api.domain.MaterializationWriteCapability;
import org.pangrova.materialization.api.enums.ObjectState;
import org.pangrova.materialization.api.enums.PinState;
import org.pangrova.materialization.api.domain.ReaderId;
import org.pangrova.materialization.api.domain.ReaderPin;
import org.pangrova.materialization.api.domain.RefreshAttempt;
import org.pangrova.materialization.api.domain.Schedule;
import org.pangrova.materialization.api.domain.SourcePin;
import org.pangrova.materialization.api.domain.TenantId;
import org.pangrova.materialization.api.port.Clock;
import org.pangrova.materialization.api.port.CommandRecord;
import org.pangrova.materialization.api.port.GcHistoryEntry;
import org.pangrova.materialization.api.port.MaterializationEvent;
import org.pangrova.persistence.materialization.api.lifecycle.MaterializationRepository;
import org.pangrova.materialization.api.port.PublicationResult;
import org.pangrova.materialization.api.port.PublishCommand;
import org.pangrova.materialization.api.port.ReclaimableSourcePin;
import org.pangrova.connector.api.materialization.CommitEvidence;
import org.pangrova.materialization.api.enums.ScheduleState;

/**
 * In-memory repository that mirrors the transaction and visibility semantics of
 * {@code JdbcMaterializationRepository}: object-head CAS, command idempotency,
 * generation and attempt transition guards, schedule version CAS, reader-pin
 * protection, and the collectability filter of the GC query.
 */
public final class InMemoryMaterializationRepository implements MaterializationRepository {

    private final Clock clock;
    private final Map<MaterializationId, MaterializationObject> objects = new LinkedHashMap<>();
    private final Map<MaterializationId, Map<Long, DefinitionRevision>> definitions =
            new HashMap<>();
    private final Map<GenerationId, Generation> generations = new LinkedHashMap<>();
    private final Map<AttemptId, RefreshAttempt> attempts = new LinkedHashMap<>();
    private final Map<MaterializationId, Schedule> schedules = new LinkedHashMap<>();
    private final Map<String, CommandRecord> commands = new LinkedHashMap<>();
    private final Map<String, PublicationResult> publicationReceipts = new HashMap<>();
    private final Map<GenerationId, List<ReaderPin>> readerPins = new HashMap<>();
    private final Map<AttemptId, List<SourcePin>> pinsByAttempt = new HashMap<>();
    private final Map<AttemptId, MaterializationWriteCapability> capabilities = new HashMap<>();
    private final List<GcLedgerEntry> gcLedger = new ArrayList<>();
    private final List<MaterializationEvent> events = new ArrayList<>();
    private final java.util.Set<String> liveAnchors = new java.util.HashSet<>();
    private boolean failNextScheduleUpdate;

    public InMemoryMaterializationRepository(Clock clock) {
        this.clock = clock;
    }

    public void putObject(MaterializationObject object) {
        objects.put(object.id(), object);
    }

    public void putDefinition(DefinitionRevision definition) {
        definitions.computeIfAbsent(definition.object(), key -> new HashMap<>())
                .put(definition.revision(), definition);
    }

    public void putGeneration(Generation generation) {
        generations.put(generation.id(), generation);
    }

    public void putAttempt(RefreshAttempt attempt) {
        attempts.put(attempt.id(), attempt);
    }

    public void putSchedule(Schedule schedule) {
        schedules.put(schedule.object(), schedule);
    }

    public List<MaterializationEvent> events() {
        return List.copyOf(events);
    }

    /** Models a coordinator execution anchor that is still being renewed. */
    public void putLiveAnchor(String anchor) {
        liveAnchors.add(anchor);
    }

    /** Models an execution anchor whose owner disappeared. */
    public void removeAnchor(String anchor) {
        liveAnchors.remove(anchor);
    }

    public List<GcLedgerEntry> gcLedger() {
        return List.copyOf(gcLedger);
    }

    public Optional<MaterializationObject> storedObject(MaterializationId object) {
        return Optional.ofNullable(objects.get(object));
    }

    /** Emulates a lost settlement write so the caller keeps its unversioned claim. */
    public void failNextScheduleUpdate() {
        this.failNextScheduleUpdate = true;
    }

    @Override
    public Optional<MaterializationObject> findObject(MaterializationId object) {
        return Optional.ofNullable(objects.get(object));
    }

    @Override
    public Optional<DefinitionRevision> findDefinition(MaterializationId object, long revision) {
        return Optional.ofNullable(definitions.getOrDefault(object, Map.of()).get(revision));
    }

    @Override
    public List<MaterializationId> findLiveDependents(MaterializationId upstream) {
        return objects.values().stream()
                .filter(object -> object.state() != ObjectState.DROPPING
                        && object.state() != ObjectState.DROPPED)
                .filter(object -> {
                    DefinitionRevision definition = definitions
                            .getOrDefault(object.id(), Map.of())
                            .get(object.definitionRevision());
                    return definition != null && definition.dependencies().stream()
                            .anyMatch(dependency -> dependency.upstream().equals(upstream));
                })
                .map(MaterializationObject::id)
                .sorted(Comparator.comparing(MaterializationId::canonicalName))
                .toList();
    }

    private void advanceDependentSchedules(MaterializationId upstream, Instant now) {
        for (MaterializationId dependent : findLiveDependents(upstream)) {
            MaterializationObject object = objects.get(dependent);
            Schedule schedule = schedules.get(dependent);
            if (object == null || object.state() != ObjectState.ACTIVE
                    || schedule == null) {
                continue;
            }
            Schedule advanced = schedule.advanceDueAt(now);
            if (advanced.version() != schedule.version()) {
                schedules.put(dependent, advanced);
            }
        }
    }

    @Override
    public List<MaterializationObject> listObjects(TenantId tenant) {
        return objects.values().stream()
                .filter(object -> object.id().tenant().equals(tenant))
                .sorted(Comparator.comparing(object -> object.id().canonicalName()))
                .toList();
    }

    @Override
    public List<RefreshAttempt> findAttempts(TenantId tenant,
            Optional<MaterializationId> object, int limit) {
        requireLimit(limit);
        return attempts.values().stream()
                .filter(attempt -> attempt.object().tenant().equals(tenant))
                .filter(attempt -> object.isEmpty() || object.orElseThrow().equals(attempt.object()))
                .sorted(Comparator.comparing(RefreshAttempt::createdAt).reversed()
                        .thenComparing(attempt -> attempt.id().value().toString()))
                .limit(limit)
                .toList();
    }

    @Override
    public List<Dependency> findDependencies(TenantId tenant,
            Optional<MaterializationId> object) {
        return objects.values().stream()
                .filter(candidate -> candidate.id().tenant().equals(tenant))
                .flatMap(candidate -> {
                    DefinitionRevision definition = definitions
                            .getOrDefault(candidate.id(), Map.of())
                            .get(candidate.definitionRevision());
                    return definition == null ? java.util.stream.Stream.empty()
                            : definition.dependencies().stream();
                })
                .filter(edge -> object.isEmpty() || object.orElseThrow().equals(edge.upstream())
                        || object.orElseThrow().equals(edge.downstream()))
                .sorted(Comparator.comparing((Dependency edge) -> edge.downstream().canonicalName())
                        .thenComparingLong(Dependency::downstreamDefinitionRevision)
                        .thenComparing(edge -> edge.upstream().canonicalName()))
                .toList();
    }

    @Override
    public List<GcHistoryEntry> findGcHistory(TenantId tenant,
            Optional<MaterializationId> object, int limit) {
        requireLimit(limit);
        return gcLedger.stream()
                .map(entry -> {
                    Generation generation = generations.get(entry.generation());
                    return generation == null ? null
                            : new GcHistoryEntry(generation.object(), entry.entryId(),
                                    entry.generation(), entry.decision(), entry.evidence(),
                                    entry.state(), entry.createdAt(), entry.updatedAt());
                })
                .filter(entry -> entry != null && entry.object().tenant().equals(tenant))
                .filter(entry -> object.isEmpty() || object.orElseThrow().equals(entry.object()))
                .sorted(Comparator.comparing(GcHistoryEntry::createdAt).reversed()
                        .thenComparing(entry -> entry.entryId().toString()))
                .limit(limit)
                .toList();
    }

    private static void requireLimit(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }
    }

    @Override
    public Optional<Generation> findGeneration(GenerationId generation) {
        return Optional.ofNullable(generations.get(generation));
    }

    @Override
    public Optional<Generation> findPublishedGeneration(MaterializationId object) {
        return findObject(object)
                .flatMap(MaterializationObject::publishedGeneration)
                .map(generations::get);
    }

    @Override
    public List<Generation> findGenerations(MaterializationId object) {
        return generations.values().stream()
                .filter(generation -> generation.object().equals(object))
                .sorted(Comparator.comparing(Generation::createdAt).thenComparing(
                        generation -> generation.id().value()))
                .toList();
    }

    @Override
    public Optional<RefreshAttempt> findAttempt(AttemptId attempt) {
        return Optional.ofNullable(attempts.get(attempt));
    }

    @Override
    public Optional<RefreshAttempt> findActiveAttempt(MaterializationId object) {
        return attempts.values().stream()
                .filter(attempt -> attempt.object().equals(object))
                .filter(attempt -> isActive(attempt.state()))
                .findFirst();
    }

    @Override
    public Optional<Generation> findGenerationByAttempt(AttemptId attempt) {
        return generations.values().stream()
                .filter(generation -> generation.producingAttempt().equals(attempt))
                .findFirst();
    }

    @Override
    public List<RefreshAttempt> findReconcilableAttempts(Instant observedBefore) {
        return attempts.values().stream()
                .filter(attempt -> isActive(attempt.state()))
                .filter(attempt -> !liveAnchors.contains(attempt.fence().executionAnchor())
                        || (attempt.state() == AttemptState.QUARANTINED
                                && attempt.updatedAt().isBefore(observedBefore)))
                .sorted(Comparator.comparing(RefreshAttempt::updatedAt)
                        .thenComparing(attempt -> attempt.id().value()))
                .toList();
    }

    private static boolean isActive(AttemptState state) {
        return state == AttemptState.PREPARING
                || state == AttemptState.BUILDING
                || state == AttemptState.COMMITTED
                || state == AttemptState.QUARANTINED;
    }

    @Override
    public void markAttemptBuilding(AttemptId attempt, Instant now) {
        RefreshAttempt current = attempts.get(attempt);
        if (current == null) {
            throw MaterializationException.requestRejected(
                    "attempt " + attempt.value() + " does not exist");
        }
        if (current.state() != AttemptState.PREPARING
                && current.state() != AttemptState.BUILDING) {
            throw MaterializationException.concurrentPublication(
                    "attempt " + attempt.value() + " is no longer preparing");
        }
        attempts.put(attempt, current.markBuilding(now));
    }

    @Override
    public Optional<Schedule> findSchedule(MaterializationId object) {
        return Optional.ofNullable(schedules.get(object));
    }

    @Override
    public Optional<CommandRecord> findCommand(TenantId tenant, CommandId commandId) {
        return Optional.ofNullable(commands.get(commandKey(tenant, commandId)));
    }

    @Override
    public void createObject(MaterializationObject object, DefinitionRevision definition,
            Schedule schedule, CommandId commandId, MaterializationEvent event) {
        putObject(object);
        putDefinition(definition);
        putSchedule(schedule);
        recordCommand(new CommandRecord(object.id().tenant(), commandId,
                reference(object.id(), "created"), clock.now()));
        events.add(event);
    }

    @Override
    public boolean createObjectIfAbsent(MaterializationObject object,
            DefinitionRevision definition, Schedule schedule, CommandId commandId,
            MaterializationEvent event) {
        if (objects.containsKey(object.id())) {
            return false;
        }
        createObject(object, definition, schedule, commandId, event);
        return true;
    }

    @Override
    public boolean reclaimDroppedObject(MaterializationObject expectedDropped,
            MaterializationObject replacement, DefinitionRevision definition, Schedule schedule,
            CommandId commandId, MaterializationEvent event) {
        MaterializationObject current = objects.get(replacement.id());
        if (current == null || current.state() != ObjectState.DROPPED
                || !current.incarnation().equals(expectedDropped.incarnation())) {
            return false;
        }
        definitions.remove(replacement.id());
        schedules.remove(replacement.id());
        createObject(replacement, definition, schedule, commandId, event);
        return true;
    }

    @Override
    public void renameObject(MaterializationObject object, MaterializationId previous,
            long expectedRevision, CommandId commandId, MaterializationEvent event) {
        MaterializationObject current = objects.get(previous);
        if (current == null) {
            throw MaterializationException.notFound(previous.canonicalName());
        }
        if (current.revision() != expectedRevision || object.revision() != expectedRevision + 1) {
            throw MaterializationException.concurrentPublication(
                    "expected head revision " + expectedRevision + " but found "
                            + current.revision() + " for " + previous.canonicalName());
        }
        objects.remove(previous);
        objects.put(object.id(), object);

        Map<Long, DefinitionRevision> revisions = definitions.remove(previous);
        if (revisions != null) {
            Map<Long, DefinitionRevision> renamed = new HashMap<>();
            for (var entry : revisions.entrySet()) {
                DefinitionRevision definition = entry.getValue();
                var dependencies = definition.dependencies().stream()
                        .map(dependency -> new Dependency(
                                dependency.upstream().equals(previous)
                                        ? object.id() : dependency.upstream(),
                                object.id(), dependency.downstreamDefinitionRevision(),
                                dependency.kind()))
                        .collect(java.util.stream.Collectors.toCollection(
                                java.util.LinkedHashSet::new));
                renamed.put(entry.getKey(), new DefinitionRevision(object.id(),
                        definition.revision(), definition.sql(), definition.binding(),
                        definition.outputSchema(), definition.sources(), dependencies,
                        definition.policySnapshot(), definition.securityMode(),
                        definition.definer(), definition.definerGroups(),
                        definition.sessionProperties(), definition.createdAt()));
            }
            definitions.put(object.id(), renamed);
        }

        Schedule schedule = schedules.remove(previous);
        if (schedule != null) {
            schedules.put(object.id(), new Schedule(object.id(), schedule.targetLag(),
                    schedule.nextDueAt(), schedule.lastSuccessAt(),
                    schedule.consecutiveFailures(), schedule.state(), schedule.lease(),
                    schedule.version()));
        }

        generations.replaceAll((id, generation) -> generation.object().equals(previous)
                ? new Generation(generation.id(), object.id(), generation.incarnation(),
                        generation.definitionRevision(), generation.producingAttempt(),
                        generation.baseGeneration(), generation.physicalTable(),
                        generation.refreshKind(), generation.sourceVector(),
                        generation.evidence(), generation.state(), generation.createdAt(),
                        generation.publishedAt())
                : generation);
        attempts.replaceAll((id, attempt) -> attempt.object().equals(previous)
                ? new RefreshAttempt(attempt.id(), object.id(), attempt.incarnation(),
                        attempt.definitionRevision(), attempt.baseObjectRevision(),
                        attempt.baseGeneration(), attempt.refreshKind(), attempt.sourceVector(),
                        attempt.trigger(), attempt.requesting(), attempt.definer(),
                        attempt.fence(), attempt.candidateTable(), attempt.state(),
                        attempt.evidence(), attempt.errorCode(), attempt.createdAt(),
                        attempt.updatedAt())
                : attempt);
        capabilities.replaceAll((id, capability) -> capability.object().equals(previous)
                ? new MaterializationWriteCapability(object.id(), capability.incarnation(),
                        capability.definitionRevision(), capability.attempt(),
                        capability.candidateTable(), capability.generation(),
                        capability.sourceVectorHash(), capability.definer(),
                        capability.controlEpoch(), capability.issuedAt(),
                        capability.expiresAt())
                : capability);

        recordCommand(new CommandRecord(object.id().tenant(), commandId,
                reference(object.id(), "renamed"), clock.now()));
        events.add(event);
    }

    @Override
    public void recordDefinition(DefinitionRevision definition) {
        MaterializationObject object = objects.get(definition.object());
        if (object == null) {
            throw MaterializationException.notFound(definition.object().canonicalName());
        }
        long highest = definitions.getOrDefault(definition.object(), Map.of()).keySet().stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L);
        if (definition.revision() != highest + 1) {
            throw MaterializationException.invalidDefinition(
                    "definition revision " + definition.revision()
                            + " does not follow " + highest);
        }
        putDefinition(definition);
    }

    @Override
    public void replaceDefinition(DefinitionRevision definition, MaterializationObject object,
            long expectedRevision, CommandId commandId, MaterializationEvent event) {
        MaterializationObject current = objects.get(object.id());
        if (current == null) {
            throw MaterializationException.notFound(object.id().canonicalName());
        }
        if (current.revision() != expectedRevision) {
            throw MaterializationException.concurrentPublication(
                    "expected head revision " + expectedRevision + " but found "
                            + current.revision() + " for " + object.id().canonicalName());
        }
        if (object.revision() != expectedRevision + 1) {
            throw MaterializationException.concurrentPublication(
                    "mutation must advance the head revision for " + object.id().canonicalName());
        }
        if (current.state() != ObjectState.ACTIVE
                && current.state() != ObjectState.SUSPENDED) {
            throw MaterializationException.invalidStateTransition(
                    "cannot redefine an object in state " + current.state().stateName());
        }
        if (!definition.object().equals(object.id())
                || definition.revision() != current.definitionRevision() + 1
                || definition.revision() != object.definitionRevision()) {
            throw MaterializationException.invalidDefinition(
                    "definition revision must advance exactly one for "
                            + object.id().canonicalName());
        }
        long highest = definitions.getOrDefault(definition.object(), Map.of()).keySet().stream()
                .mapToLong(Long::longValue)
                .max()
                .orElse(0L);
        if (definition.revision() != highest + 1) {
            throw MaterializationException.invalidDefinition(
                    "definition revision " + definition.revision() + " does not follow " + highest);
        }
        putDefinition(definition);
        objects.put(object.id(), object);
        recordCommand(new CommandRecord(object.id().tenant(), commandId,
                reference(object.id(), "redefined"), clock.now()));
        events.add(event);
    }

    @Override
    public void updateObject(MaterializationObject object, long expectedRevision,
            CommandId commandId, MaterializationEvent event) {
        MaterializationObject current = objects.get(object.id());
        if (current == null) {
            throw MaterializationException.notFound(object.id().canonicalName());
        }
        if (current.revision() != expectedRevision) {
            throw MaterializationException.concurrentPublication(
                    "expected head revision " + expectedRevision + " but found "
                            + current.revision() + " for " + object.id().canonicalName());
        }
        if (object.revision() != expectedRevision + 1) {
            throw MaterializationException.concurrentPublication(
                    "mutation must advance the head revision for " + object.id().canonicalName());
        }
        objects.put(object.id(), object);
        recordCommand(new CommandRecord(object.id().tenant(), commandId,
                reference(object.id(), "updated"), clock.now()));
        events.add(event);
    }

    @Override
    public void reserveAttempt(RefreshAttempt attempt, Generation candidate,
            List<SourcePin> sourcePins, MaterializationWriteCapability capability,
            long expectedObjectRevision, MaterializationEvent event) {
        MaterializationObject object = objects.get(attempt.object());
        if (object == null) {
            throw MaterializationException.notFound(attempt.object().canonicalName());
        }
        if (object.revision() != expectedObjectRevision) {
            throw MaterializationException.concurrentPublication(
                    "expected head revision " + expectedObjectRevision + " but found "
                            + object.revision() + " for " + attempt.object().canonicalName());
        }
        if (findActiveAttempt(attempt.object()).isPresent()) {
            throw MaterializationException.concurrentPublication(
                    "an active attempt already exists for " + attempt.object().canonicalName());
        }
        putAttempt(attempt);
        putGeneration(candidate);
        pinsByAttempt.put(attempt.id(), new ArrayList<>(sourcePins));
        capabilities.put(attempt.id(), capability);
        events.add(event);
    }

    /** Returns the durable pin history of an attempt, including released pins. */
    public List<SourcePin> sourcePins(AttemptId attempt) {
        return List.copyOf(pinsByAttempt.getOrDefault(attempt, List.of()));
    }

    @Override
    public List<ReclaimableSourcePin> findReclaimableSourcePins(
            Instant now, Instant settledBefore, int attemptLimit) {
        if (attemptLimit < 1) {
            throw new IllegalArgumentException("attemptLimit must be positive");
        }
        List<AttemptId> selected = pinsByAttempt.entrySet().stream()
                .filter(entry -> hasReclaimablePin(
                        entry.getKey(), entry.getValue(), now, settledBefore))
                .sorted(Comparator.comparing(entry -> earliestExpiry(entry.getValue())))
                .map(Map.Entry::getKey)
                .limit(attemptLimit)
                .toList();
        List<ReclaimableSourcePin> result = new ArrayList<>();
        for (AttemptId attemptId : selected) {
            RefreshAttempt attempt = attempts.get(attemptId);
            for (SourcePin pin : pinsByAttempt.getOrDefault(attemptId, List.of())) {
                if (pin.state() == PinState.ACQUIRED) {
                    result.add(new ReclaimableSourcePin(attempt.object().tenant(), attemptId,
                            pin.source(), pin.snapshot()));
                }
            }
        }
        return result;
    }

    private boolean hasReclaimablePin(AttemptId attemptId, List<SourcePin> pins,
            Instant now, Instant settledBefore) {
        RefreshAttempt attempt = attempts.get(attemptId);
        if (attempt == null) {
            return false;
        }
        boolean settled = isSettled(attempt.state())
                && attempt.updatedAt().isBefore(settledBefore);
        return pins.stream().anyMatch(pin -> pin.state() == PinState.ACQUIRED
                && (settled || !pin.expiresAt().isAfter(now)));
    }

    private static boolean isSettled(AttemptState state) {
        return state == AttemptState.PUBLISHED
                || state == AttemptState.FAILED
                || state == AttemptState.ABANDONED
                || state == AttemptState.SUPERSEDED
                || state == AttemptState.QUARANTINED;
    }

    private static Instant earliestExpiry(List<SourcePin> pins) {
        return pins.stream().map(SourcePin::expiresAt).min(Instant::compareTo)
                .orElse(Instant.MAX);
    }

    @Override
    public void releaseSourcePins(AttemptId attempt, Instant now) {
        List<SourcePin> pins = pinsByAttempt.get(attempt);
        if (pins == null) {
            return;
        }
        for (int index = 0; index < pins.size(); index++) {
            SourcePin pin = pins.get(index);
            if (pin.state() == PinState.ACQUIRED) {
                pins.set(index, pin.release());
            }
        }
    }

    @Override
    public Optional<MaterializationWriteCapability> findWriteCapability(AttemptId attempt) {
        return Optional.ofNullable(capabilities.get(attempt));
    }

    @Override
    public void recordCommitEvidence(AttemptId attemptId, CommitEvidence evidence, Instant now) {
        RefreshAttempt attempt = attempts.get(attemptId);
        if (attempt == null) {
            throw MaterializationException.requestRejected(
                    "attempt " + attemptId.value() + " does not exist");
        }
        if (attempt.state() != AttemptState.PREPARING
                && attempt.state() != AttemptState.BUILDING
                && attempt.state() != AttemptState.QUARANTINED
                && attempt.state() != AttemptState.COMMITTED) {
            throw MaterializationException.evidenceMismatch(
                    "attempt " + attemptId.value() + " in state " + attempt.state()
                            + " cannot accept commit evidence");
        }
        if (attempt.evidence().isPresent()) {
            if (!attempt.evidence().orElseThrow().equals(evidence)) {
                throw MaterializationException.evidenceMismatch(
                        "attempt " + attemptId.value() + " already carries different evidence");
            }
        } else {
            RefreshAttempt ready = attempt;
            if (ready.state() == AttemptState.PREPARING) {
                ready = ready.markBuilding(now);
            }
            putAttempt(ready.markCommitted(evidence, now));
        }
        Generation generation = findGenerationByAttempt(attemptId)
                .orElseThrow(() -> MaterializationException.requestRejected(
                        "generation of attempt " + attemptId.value() + " does not exist"));
        Generation committed = generation.state() == GenerationState.COMMITTED
                ? generation
                : generation.commit(evidence, now);
        putGeneration(committed);
    }

    @Override
    public PublicationResult publish(PublishCommand command, MaterializationEvent event) {
        Optional<CommandRecord> receipt = findCommand(
                command.object().tenant(), command.commandId());
        if (receipt.isPresent() && publicationReceipts.containsKey(
                commandKey(command.object().tenant(), command.commandId()))) {
            PublicationResult recorded = publicationReceipts.get(
                    commandKey(command.object().tenant(), command.commandId()));
            return new PublicationResult(recorded.object(), recorded.generation(), false);
        }
        MaterializationObject current = objects.get(command.object());
        if (current == null) {
            throw MaterializationException.notFound(command.object().canonicalName());
        }
        Instant now = clock.now();
        if (current.revision() != command.expectedObjectRevision()) {
            throw MaterializationException.concurrentPublication(
                    "expected head revision " + command.expectedObjectRevision()
                            + " but found " + current.revision() + " for "
                            + command.object().canonicalName());
        }
        command.fence().requireValidAt(now);
        RefreshAttempt attempt = attempts.get(command.attempt().id());
        if (attempt == null || attempt.state() != AttemptState.COMMITTED) {
            throw MaterializationException.evidenceMismatch(
                    "attempt " + command.attempt().id().value() + " is not committed");
        }
        if (attempt.evidence().isEmpty()
                || !attempt.evidence().orElseThrow().equals(command.evidence())) {
            throw MaterializationException.evidenceMismatch(
                    "publication evidence does not match the recorded attempt evidence");
        }
        Generation generation = generations.get(command.generation().id());
        if (generation == null || generation.state() != GenerationState.COMMITTED
                || !generation.producingAttempt().equals(attempt.id())) {
            throw MaterializationException.evidenceMismatch(
                    "generation " + command.generation().id().value()
                            + " is not the committed candidate of its attempt");
        }
        MaterializationObject desired = current.publish(
                generation.id(), current.revision(), now);
        current.publishedGeneration()
                .filter(previous -> !previous.equals(generation.id()))
                .map(generations::get)
                .filter(previous -> previous != null
                        && previous.state() == GenerationState.PUBLISHED)
                .ifPresent(previous -> generations.put(previous.id(), previous.retire()));
        Generation published = generation.publish(now);
        generations.put(published.id(), published);
        attempts.put(attempt.id(), attempt.markPublished(now));
        objects.put(desired.id(), desired);
        advanceDependentSchedules(desired.id(), now);
        recordCommand(new CommandRecord(command.object().tenant(), command.commandId(),
                reference(command.object(), "published"), now));
        events.add(event);
        PublicationResult result = new PublicationResult(desired, published, true);
        publicationReceipts.put(commandKey(command.object().tenant(), command.commandId()),
                result);
        return result;
    }

    @Override
    public void settleAttempt(RefreshAttempt previous, RefreshAttempt attempt,
            Generation generation, MaterializationEvent event) {
        RefreshAttempt current = attempts.get(previous.id());
        if (current == null) {
            throw MaterializationException.requestRejected(
                    "attempt " + previous.id().value() + " does not exist");
        }
        if (!generations.containsKey(generation.id())) {
            throw MaterializationException.requestRejected(
                    "generation " + generation.id().value() + " does not exist");
        }
        if (attempt.updatedAt().isBefore(previous.updatedAt())) {
            throw MaterializationException.requestRejected(
                    "attempt update instant must not move backwards");
        }
        if (current.state() != previous.state()) {
            throw MaterializationException.concurrentPublication(
                    "attempt " + previous.id().value() + " is no longer in state "
                            + previous.state());
        }
        attempts.put(attempt.id(), attempt);
        generations.put(generation.id(), generation);
        events.add(event);
    }

    @Override
    public void dropObject(MaterializationObject dropped,
            Optional<GenerationId> publishedGeneration, long expectedRevision,
            CommandId commandId, MaterializationEvent event) {
        MaterializationObject current = objects.get(dropped.id());
        if (current == null) {
            throw MaterializationException.notFound(dropped.id().canonicalName());
        }
        if (current.revision() != expectedRevision || dropped.revision() != expectedRevision + 1) {
            throw MaterializationException.concurrentPublication(
                    "object header for " + dropped.id().canonicalName()
                            + " is no longer dropping at revision " + expectedRevision);
        }
        if (current.state() != ObjectState.DROPPING) {
            throw MaterializationException.invalidStateTransition(
                    "object " + dropped.id().canonicalName() + " is not dropping");
        }
        objects.put(dropped.id(), dropped);
        if (publishedGeneration.isPresent()) {
            generations.computeIfPresent(publishedGeneration.orElseThrow(),
                    (id, generation) -> generation.state() == GenerationState.PUBLISHED
                            ? generation.retire() : generation);
        }
        Instant now = dropped.updatedAt();
        for (RefreshAttempt attempt : List.copyOf(attempts.values())) {
            if (!attempt.object().equals(dropped.id())) {
                continue;
            }
            if (attempt.state() == AttemptState.COMMITTED) {
                attempts.put(attempt.id(), attempt.supersede("object dropped", now));
            } else if (isActive(attempt.state())) {
                attempts.put(attempt.id(), attempt.abandon("object dropped", now));
            }
        }
        recordCommand(new CommandRecord(dropped.id().tenant(), commandId,
                reference(dropped.id(), "dropped"), now));
        events.add(event);
    }

    @Override
    public boolean collectGeneration(Generation generation, GcLedgerEntry entry,
            MaterializationEvent event, Runnable physicalDrop) {
        Generation current = generations.get(generation.id());
        if (current == null || current.state() != generation.state()) {
            return false;
        }
        Instant now = clock.now();
        boolean pinned = readerPins.getOrDefault(generation.id(), List.of()).stream()
                .anyMatch(pin -> pin.isActiveAt(now));
        if (pinned || hasLiveBase(generation) || publishedGeneration(generation.id())) {
            return false;
        }
        physicalDrop.run();
        generations.put(generation.id(), generation.collect());
        updateGcDecision(entry.entryId(), GcState.APPLIED, event.occurredAt());
        events.add(event);
        return true;
    }

    private boolean hasLiveBase(Generation generation) {
        return generations.values().stream()
                .filter(other -> !other.id().equals(generation.id()))
                .filter(other -> other.baseGeneration().filter(generation.id()::equals).isPresent())
                .anyMatch(other -> other.state() == GenerationState.CANDIDATE
                        || other.state() == GenerationState.COMMITTED
                        || other.state() == GenerationState.PUBLISHED
                        || other.state() == GenerationState.QUARANTINED);
    }

    private boolean publishedGeneration(GenerationId generation) {
        return objects.values().stream()
                .anyMatch(object -> object.publishedGeneration()
                        .filter(generation::equals)
                        .isPresent());
    }

    @Override
    public void saveGeneration(Generation generation, MaterializationEvent event) {
        if (!generations.containsKey(generation.id())) {
            throw MaterializationException.requestRejected(
                    "generation " + generation.id().value() + " does not exist");
        }
        generations.put(generation.id(), generation);
        events.add(event);
    }

    @Override
    public void updateSchedule(Schedule schedule, long expectedVersion) {
        if (failNextScheduleUpdate) {
            failNextScheduleUpdate = false;
            throw MaterializationException.repositoryFailure(
                    "simulated schedule settlement failure", null);
        }
        Schedule current = schedules.get(schedule.object());
        if (current == null) {
            throw MaterializationException.requestRejected(
                    "schedule for " + schedule.object().canonicalName() + " does not exist");
        }
        if (current.version() != expectedVersion) {
            throw MaterializationException.concurrentPublication(
                    "schedule CAS lost for " + schedule.object().canonicalName());
        }
        schedules.put(schedule.object(), schedule);
    }

    @Override
    public Optional<Schedule> claimDueSchedule(String owner, Instant now,
            Duration leaseDuration) {
        return schedules.values().stream()
                .filter(schedule -> schedule.state() == org.pangrova.materialization.api.enums.ScheduleState.ACTIVE)
                .filter(schedule -> schedule.nextDueAt()
                        .map(due -> !due.isAfter(now))
                        .orElse(false))
                .filter(schedule -> schedule.lease()
                        .map(lease -> lease.isExpiredAt(now))
                        .orElse(true))
                .sorted(Comparator.comparing(
                                (Schedule schedule) -> schedule.nextDueAt().orElse(Instant.MIN))
                        .thenComparing((Schedule schedule) -> schedule.object().canonicalName()))
                .findFirst()
                .map(schedule -> {
                    Schedule claimed = schedule.claim(owner, now, leaseDuration);
                    schedules.put(claimed.object(), claimed);
                    return claimed;
                });
    }

    @Override
    public void recordCommand(CommandRecord record) {
        commands.put(commandKey(record.tenant(), record.commandId()), record);
    }

    @Override
    public void pinReader(ReaderPin pin) {
        List<ReaderPin> pins = readerPins.computeIfAbsent(pin.generation(), key -> new ArrayList<>());
        for (int index = 0; index < pins.size(); index++) {
            ReaderPin existing = pins.get(index);
            if (existing.reader().equals(pin.reader())) {
                pins.set(index, new ReaderPin(existing.generation(), existing.reader(),
                        existing.owner(),
                        later(existing.expiresAt(), pin.expiresAt()),
                        later(existing.heartbeatAt(), pin.heartbeatAt())));
                return;
            }
        }
        pins.add(pin);
    }

    @Override
    public void releaseReaderPin(GenerationId generation, ReaderId reader) {
        readerPins.getOrDefault(generation, new ArrayList<>())
                .removeIf(pin -> pin.reader().equals(reader));
    }

    @Override
    public void releaseReaderPins(ReaderId reader) {
        readerPins.values().forEach(pins ->
                pins.removeIf(pin -> pin.reader().equals(reader)));
    }

    @Override
    public void renewReaderPins(ReaderId reader, Instant expiresAt, Instant heartbeatAt) {
        readerPins.replaceAll((generation, pins) -> {
            List<ReaderPin> renewed = new ArrayList<>(pins.size());
            for (ReaderPin pin : pins) {
                renewed.add(pin.reader().equals(reader)
                        ? new ReaderPin(pin.generation(), pin.reader(), pin.owner(),
                                later(pin.expiresAt(), expiresAt),
                                later(pin.heartbeatAt(), heartbeatAt))
                        : pin);
            }
            return renewed;
        });
    }

    @Override
    public int purgeExpiredReaderPins(Instant now) {
        int[] removed = {0};
        readerPins.replaceAll((generation, pins) -> {
            List<ReaderPin> retained = new ArrayList<>(pins.size());
            for (ReaderPin pin : pins) {
                if (pin.isActiveAt(now)) {
                    retained.add(pin);
                } else {
                    removed[0]++;
                }
            }
            return retained;
        });
        return removed[0];
    }

    private static Instant later(Instant left, Instant right) {
        return left.isAfter(right) ? left : right;
    }

    @Override
    public List<ReaderPin> findReaderPins(GenerationId generation) {
        return List.copyOf(readerPins.getOrDefault(generation, List.of()));
    }

    @Override
    public List<Generation> findCollectableGenerations(Instant now, Instant eligibleBefore,
            int limit) {
        return generations.values().stream()
                .filter(generation -> isCollectable(generation, now, eligibleBefore))
                .sorted(Comparator.comparing(
                                (Generation generation) -> generation.publishedAt()
                                        .orElse(generation.createdAt()))
                        .thenComparing(generation -> generation.id().value()))
                .limit(limit)
                .toList();
    }

    @Override
    public void recordGcDecision(GcLedgerEntry entry) {
        gcLedger.add(entry);
    }

    @Override
    public void updateGcDecision(UUID entryId, GcState state, Instant now) {
        for (int index = 0; index < gcLedger.size(); index++) {
            GcLedgerEntry entry = gcLedger.get(index);
            if (entry.entryId().equals(entryId) && entry.state() != GcState.APPLIED) {
                gcLedger.set(index, new GcLedgerEntry(entry.entryId(), entry.generation(),
                        entry.decision(), entry.evidence(), state, entry.createdAt(), now));
                return;
            }
        }
    }

    private boolean isCollectable(Generation generation, Instant now, Instant eligibleBefore) {
        RefreshAttempt attempt = attempts.get(generation.producingAttempt());
        AttemptState attemptState = attempt == null ? null : attempt.state();
        boolean settled;
        if (generation.state() == GenerationState.RETIRED) {
            settled = generation.publishedAt()
                    .map(published -> !published.isAfter(eligibleBefore))
                    .orElse(false);
        } else if (generation.state() == GenerationState.CANDIDATE) {
            settled = !generation.createdAt().isAfter(eligibleBefore)
                    && (attemptState == AttemptState.FAILED
                            || attemptState == AttemptState.ABANDONED);
        } else if (generation.state() == GenerationState.COMMITTED) {
            settled = !generation.createdAt().isAfter(eligibleBefore)
                    && attemptState == AttemptState.SUPERSEDED;
        } else {
            return false;
        }
        if (!settled) {
            return false;
        }
        boolean pinned = readerPins.getOrDefault(generation.id(), List.of()).stream()
                .anyMatch(pin -> pin.isActiveAt(now));
        if (pinned) {
            return false;
        }
        boolean baseOfLiveGeneration = generations.values().stream()
                .filter(other -> !other.id().equals(generation.id()))
                .filter(other -> other.baseGeneration().filter(generation.id()::equals).isPresent())
                .anyMatch(other -> other.state() == GenerationState.CANDIDATE
                        || other.state() == GenerationState.COMMITTED
                        || other.state() == GenerationState.PUBLISHED
                        || other.state() == GenerationState.QUARANTINED);
        if (baseOfLiveGeneration) {
            return false;
        }
        return objects.values().stream()
                .noneMatch(object -> object.publishedGeneration()
                        .filter(generation.id()::equals)
                        .isPresent());
    }

    private static String commandKey(TenantId tenant, CommandId commandId) {
        return tenant.value() + '\u0000' + commandId.value();
    }

    private static String reference(MaterializationId object, String action) {
        return "{\"object\":\"" + object.canonicalName() + "\",\"action\":\"" + action + "\"}";
    }
}
