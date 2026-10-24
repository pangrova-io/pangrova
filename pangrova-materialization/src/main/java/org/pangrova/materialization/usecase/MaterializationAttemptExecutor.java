package org.pangrova.materialization.usecase;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.ExecutionFence;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.InternalNamespace;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.PartitionAlignment;
import org.pangrova.materialization.domain.PhysicalTableRef;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.QueryId;
import org.pangrova.materialization.domain.ReaderPin;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.RefreshKind;
import org.pangrova.materialization.domain.RefreshPlan;
import org.pangrova.materialization.domain.SourcePin;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.materialization.domain.TriggerKind;
import org.pangrova.materialization.extension.RefreshContext;
import org.pangrova.materialization.extension.StrategyCompatibility;
import org.pangrova.materialization.port.ExecutionRequest;
import org.pangrova.materialization.port.ExecutionResult;
import org.pangrova.materialization.port.MaterializationEventType;
import org.pangrova.materialization.port.PlannedDefinition;
import org.pangrova.materialization.port.PublicationResult;
import org.pangrova.materialization.port.PublicationVerification;
import org.pangrova.materialization.port.PublishCommand;
import org.pangrova.spi.materialization.ChangeProof;
import org.pangrova.spi.materialization.ChangeSetMode;
import org.pangrova.spi.materialization.ChangeSetPlan;
import org.pangrova.spi.materialization.MaterializationSourceException;
import org.pangrova.spi.materialization.PinnedSource;
import org.pangrova.spi.materialization.SourceRef;
import org.pangrova.spi.materialization.SourceSnapshotRef;

/**
 * Runs one reserved refresh attempt through the data plane and publishes it.
 *
 * <p>The executor owns the whole attempt lifecycle after the header and
 * definition are frozen: it pins exact source snapshots, plans and proves any
 * change set, reserves the candidate, executes it, records exact evidence, and
 * publishes under the header CAS. Every failure settles the attempt and
 * releases its holds; an ambiguous commit outcome quarantines the candidate.
 */
final class MaterializationAttemptExecutor {

    private final KernelPorts ports;
    private final Duration executionLease;
    private final Duration sourcePinTtl;

    MaterializationAttemptExecutor(KernelPorts ports, Duration executionLease,
            Duration sourcePinTtl) {
        this.ports = ports;
        this.executionLease = executionLease;
        this.sourcePinTtl = sourcePinTtl;
    }

    MaterializationOutcome execute(
            MaterializationObject header,
            DefinitionRevision definition,
            PlannedDefinition planned,
            Principal requester,
            Map<String, String> sessionProperties,
            CommandId commandId,
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

        RefreshAttempt attempt = RefreshAttempt.reserve(attemptId, header, definition, requester,
                new RefreshAttempt.AttemptScope(baseGeneration, kind, sourceVector, trigger,
                        fence, candidate), now);
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
                header.revision(), MaterializationEvents.of(MaterializationEventType.ATTEMPT_RESERVED,
                        object, requester, definition.definer(), commandId, Optional.of(attemptId),
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
            plan = resolvePlan(definition, resolution, sourceVector, candidate, held,
                    planned.partitionAlignment());
        } catch (RuntimeException failure) {
            failBeforeExecution(attempt, generation, failure);
            releasePins(held, attemptId, readerQuery, failure);
            throw failure;
        }

        Instant buildingAt = ports.clock().now();
        attempt = attempt.markBuilding(buildingAt);
        ports.repository().markAttemptBuilding(attemptId, buildingAt);

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
        RuntimeException publicationFailure = null;
        try {
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
                    MaterializationEvents.of(MaterializationEventType.GENERATION_PUBLISHED, object,
                            requester, definition.definer(), commandId, Optional.of(attemptId),
                            Optional.of(generationId), rowsMessage(result), committedAt));
            return new MaterializationOutcome(
                    published.object(), published.generation(), published.newlyPublished());
        } catch (RuntimeException failure) {
            publicationFailure = failure;
            throw failure;
        } finally {
            releasePins(held, attemptId, readerQuery, publicationFailure);
        }
    }

    private static String rowsMessage(ExecutionResult result) {
        return result.writtenRows().isPresent()
                ? "rows=" + result.writtenRows().getAsLong() : "rows=unknown";
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
            SourceVector sourceVector, PhysicalTableRef candidate, List<PinnedSource> held,
            PartitionAlignment alignment) {
        if (!resolution.kind().isIncremental()) {
            return resolution.strategy().plan(
                    new RefreshContext(definition, Optional.empty(), sourceVector, candidate));
        }
        Generation base = resolution.base().orElseThrow(
                () -> MaterializationException.incrementalUnsupported(
                        "incremental refresh requires a published base generation"));
        ChangeSetMode mode = changeSetMode(resolution.kind());
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
                                + " was materialized from an empty snapshot and has no change-set interval");
            }
            PinnedSource fromPin = hold(held, source, from.snapshotToken());
            PinnedSource toPin = hold(held, source, entry.getValue().snapshotToken());
            ChangeSetPlan changeSet = planChangeSet(fromPin, toPin, mode);
            if (proof != null) {
                throw MaterializationException.incrementalUnsupported(
                        "incremental refresh supports exactly one source change set");
            }
            changeSets.put(source, changeSet);
            proof = verifyChangeSet(changeSet);
        }
        if (proof == null) {
            throw MaterializationException.incrementalUnsupported(
                    "incremental refresh resolved no source change set");
        }
        RefreshContext context = new RefreshContext(definition, Optional.of(base), sourceVector,
                candidate, changeSets, Optional.of(proof), alignment);
        StrategyCompatibility compatibility = resolution.strategy().validate(context);
        if (!compatibility.compatible()) {
            throw MaterializationException.incrementalUnsupported(compatibility.reason());
        }
        RefreshPlan plan = resolution.strategy().plan(context);
        resolution.strategy().verify(plan, proof);
        return plan;
    }

    private ChangeSetPlan planChangeSet(PinnedSource from, PinnedSource to, ChangeSetMode mode) {
        ChangeSetPlan plan;
        try {
            plan = ports.sourcePinning().changeSet(from, to, mode);
        } catch (MaterializationSourceException failure) {
            throw MaterializationException.capabilityUnavailable(
                    "cannot plan a " + mode + " change set for " + from.source().canonicalName()
                            + ": " + failure.reason(), failure);
        }
        if (plan == null || !plan.source().equals(from.source())
                || !plan.fromSnapshot().equals(from.snapshot())
                || !plan.toSnapshot().equals(to.snapshot())
                || plan.mode() != mode) {
            throw MaterializationException.requestRejected(
                    "source provider did not return the planned " + mode
                            + " change set for " + from.source().canonicalName());
        }
        return plan;
    }

    private static ChangeSetMode changeSetMode(RefreshKind kind) {
        if (kind == RefreshKind.IncrementalAppendOnly.INSTANCE) {
            return ChangeSetMode.APPEND_ONLY;
        }
        if (kind == RefreshKind.IncrementalPartitionReplace.INSTANCE) {
            return ChangeSetMode.PARTITION_REPLACE;
        }
        throw MaterializationException.incrementalUnsupported(
                "refresh kind " + kind + " has no change-set mode");
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
            ports.repository().settleAttempt(attempt, failed, generation.collect(),
                    MaterializationEvents.of(MaterializationEventType.ATTEMPT_FAILED,
                            attempt.object(), attempt.requesting(), attempt.definer(),
                            Optional.empty(), Optional.of(attempt.id()),
                            Optional.of(generation.id()), "pre-execution preparation failed",
                            failedAt));
        } catch (RuntimeException settlementFailure) {
            failure.addSuppressed(settlementFailure);
        }
    }

    private void quarantine(RefreshAttempt attempt, Generation generation, RuntimeException failure) {
        try {
            Instant quarantinedAt = ports.clock().now();
            String reason = failure.getClass().getSimpleName();
            RefreshAttempt quarantined = attempt.quarantine(reason, quarantinedAt);
            ports.repository().settleAttempt(attempt, quarantined, generation.quarantine(),
                    MaterializationEvents.of(MaterializationEventType.ATTEMPT_QUARANTINED,
                            attempt.object(), attempt.requesting(), attempt.definer(),
                            Optional.empty(), Optional.of(attempt.id()),
                            Optional.of(generation.id()), "ambiguous commit outcome: " + reason,
                            quarantinedAt));
        } catch (RuntimeException settlementFailure) {
            failure.addSuppressed(settlementFailure);
        }
    }

    private PhysicalTableRef candidateTable(MaterializationId object) {
        String hex = ports.ids().newId().toString().replace("-", "");
        String catalog = object.name().parts().get(0);
        return new PhysicalTableRef(catalog, InternalNamespace.SCHEMA_NAME,
                InternalNamespace.candidateTableName(hex));
    }
}
