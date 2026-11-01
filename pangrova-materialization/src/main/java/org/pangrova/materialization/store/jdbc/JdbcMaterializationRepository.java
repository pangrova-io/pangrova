package org.pangrova.materialization.store.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.pangrova.materialization.domain.AttemptId;
import org.pangrova.materialization.domain.CommandId;
import org.pangrova.materialization.domain.DefinitionRevision;
import org.pangrova.materialization.domain.Dependency;
import org.pangrova.materialization.domain.GcLedgerEntry;
import org.pangrova.materialization.domain.Generation;
import org.pangrova.materialization.domain.GenerationId;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.MaterializationId;
import org.pangrova.materialization.domain.MaterializationObject;
import org.pangrova.materialization.domain.MaterializationWriteCapability;
import org.pangrova.materialization.domain.QueryId;
import org.pangrova.materialization.domain.ReaderPin;
import org.pangrova.materialization.domain.RefreshAttempt;
import org.pangrova.materialization.domain.Schedule;
import org.pangrova.materialization.domain.SourcePin;
import org.pangrova.materialization.domain.TenantId;
import org.pangrova.materialization.port.CommandRecord;
import org.pangrova.materialization.port.GcHistoryEntry;
import org.pangrova.materialization.port.MaterializationEvent;
import org.pangrova.materialization.port.MaterializationRepository;
import org.pangrova.materialization.port.PublicationResult;
import org.pangrova.materialization.port.PublishCommand;
import org.pangrova.materialization.port.ReclaimableSourcePin;
import org.pangrova.spi.materialization.CommitEvidence;

import static org.pangrova.materialization.store.jdbc.JdbcMaterializationSupport.setInstant;

/**
 * PostgreSQL authority of the unified materialization kernel.
 *
 * <p>All invariants that must survive a crashed coordinator are enforced again
 * in V68 triggers: head CAS, immutable attempt and generation identity,
 * execution fences, exact Native commit evidence, source-vector identity, and
 * legal state transitions. This class is the facade that wires the JDBC
 * adapters: object and schedule lifecycle transactions live in
 * {@link JdbcMaterializationObjectLifecycle}, attempt and generation
 * transactions in {@link JdbcMaterializationAttemptLifecycle}, row mapping and
 * read queries in {@link JdbcMaterializationRows} and
 * {@link JdbcMaterializationQueries}, publication in
 * {@link JdbcMaterializationPublications}, and reader pins in
 * {@link JdbcMaterializationReaderPins}.
 */
public final class JdbcMaterializationRepository implements MaterializationRepository {

    private final DataSource dataSource;
    private final JdbcMaterializationSql sql;
    private final JdbcMaterializationQueries queries;
    private final JdbcMaterializationPublications publications;
    private final JdbcMaterializationReaderPins readerPins;
    private final JdbcMaterializationObjectLifecycle objectLifecycle;
    private final JdbcMaterializationAttemptLifecycle attemptLifecycle;

    /**
     * Creates the repository over the metadata connection pool.
     *
     * @param dataSource pool that owns the materialization schema
     */
    public JdbcMaterializationRepository(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource must not be null");
        }
        this.dataSource = dataSource;
        this.sql = new JdbcMaterializationSql(dataSource);
        this.queries = new JdbcMaterializationQueries(sql);
        this.publications = new JdbcMaterializationPublications(sql, this);
        this.readerPins = new JdbcMaterializationReaderPins(dataSource);
        this.objectLifecycle = new JdbcMaterializationObjectLifecycle(sql, dataSource);
        this.attemptLifecycle = new JdbcMaterializationAttemptLifecycle(sql, queries, readerPins);
    }

    @Override
    public void createObject(MaterializationObject object, DefinitionRevision definition,
            Schedule schedule, CommandId commandId, MaterializationEvent event) {
        objectLifecycle.createObject(object, definition, schedule, commandId, event);
    }

    @Override
    public boolean createObjectIfAbsent(MaterializationObject object,
            DefinitionRevision definition, Schedule schedule, CommandId commandId,
            MaterializationEvent event) {
        return objectLifecycle.createObjectIfAbsent(object, definition, schedule, commandId, event);
    }

    @Override
    public boolean reclaimDroppedObject(MaterializationObject expectedDropped,
            MaterializationObject replacement, DefinitionRevision definition, Schedule schedule,
            CommandId commandId, MaterializationEvent event) {
        return objectLifecycle.reclaimDroppedObject(
                expectedDropped, replacement, definition, schedule, commandId, event);
    }

    @Override
    public void renameObject(MaterializationObject object, MaterializationId previous,
            long expectedRevision, CommandId commandId, MaterializationEvent event) {
        objectLifecycle.renameObject(object, previous, expectedRevision, commandId, event);
    }

    @Override
    public void recordDefinition(DefinitionRevision definition) {
        objectLifecycle.recordDefinition(definition);
    }

    @Override
    public void replaceDefinition(DefinitionRevision definition, MaterializationObject object,
            long expectedRevision, CommandId commandId, MaterializationEvent event) {
        objectLifecycle.replaceDefinition(definition, object, expectedRevision, commandId, event);
    }

    @Override
    public void updateObject(MaterializationObject object, long expectedRevision,
            CommandId commandId, MaterializationEvent event) {
        objectLifecycle.updateObject(object, expectedRevision, commandId, event);
    }

    @Override
    public void reserveAttempt(RefreshAttempt attempt, Generation candidate, List<SourcePin> sourcePins,
            MaterializationWriteCapability capability, long expectedObjectRevision,
            MaterializationEvent event) {
        attemptLifecycle.reserveAttempt(
                attempt, candidate, sourcePins, capability, expectedObjectRevision, event);
    }

    @Override
    public void settleAttempt(RefreshAttempt previous, RefreshAttempt attempt, Generation generation,
            MaterializationEvent event) {
        attemptLifecycle.settleAttempt(previous, attempt, generation, event);
    }

    @Override
    public void dropObject(MaterializationObject dropped,
            Optional<GenerationId> publishedGeneration, long expectedRevision, CommandId commandId,
            MaterializationEvent event) {
        objectLifecycle.dropObject(dropped, publishedGeneration, expectedRevision, commandId, event);
    }

    @Override
    public boolean collectGeneration(Generation generation, GcLedgerEntry entry,
            MaterializationEvent event, Runnable physicalDrop) {
        return attemptLifecycle.collectGeneration(generation, entry, event, physicalDrop);
    }

    @Override
    public void saveGeneration(Generation generation, MaterializationEvent event) {
        attemptLifecycle.saveGeneration(generation, event);
    }

    @Override
    public void updateSchedule(Schedule schedule, long expectedVersion) {
        objectLifecycle.updateSchedule(schedule, expectedVersion);
    }

    @Override
    public Optional<Schedule> claimDueSchedule(String owner, Instant now, Duration leaseDuration) {
        return objectLifecycle.claimDueSchedule(owner, now, leaseDuration);
    }

    @Override
    public void recordCommand(CommandRecord record) {
        try (Connection connection = dataSource.getConnection()) {
            JdbcMaterializationWrites.recordCommand(connection, record);
        } catch (SQLException failure) {
            throw MaterializationException.repositoryFailure("recordCommand failed", failure);
        }
    }

    @Override
    public Optional<MaterializationObject> findObject(MaterializationId object) {
        return queries.findObject(object);
    }

    @Override
    public Optional<DefinitionRevision> findDefinition(MaterializationId object, long revision) {
        return queries.findDefinition(object, revision);
    }

    @Override
    public List<MaterializationId> findLiveDependents(MaterializationId upstream) {
        return queries.findLiveDependents(upstream);
    }

    @Override
    public List<MaterializationObject> listObjects(TenantId tenant) {
        return queries.listObjects(tenant);
    }

    @Override
    public List<RefreshAttempt> findAttempts(TenantId tenant,
            Optional<MaterializationId> object, int limit) {
        return queries.findAttempts(tenant, object, limit);
    }

    @Override
    public List<Dependency> findDependencies(TenantId tenant,
            Optional<MaterializationId> object) {
        return queries.findDependencies(tenant, object);
    }

    @Override
    public List<GcHistoryEntry> findGcHistory(TenantId tenant,
            Optional<MaterializationId> object, int limit) {
        return queries.findGcHistory(tenant, object, limit);
    }

    @Override
    public Optional<Generation> findGeneration(GenerationId generation) {
        return queries.findGeneration(generation);
    }

    @Override
    public Optional<Generation> findPublishedGeneration(MaterializationId object) {
        return queries.findPublishedGeneration(object);
    }

    @Override
    public List<Generation> findGenerations(MaterializationId object) {
        return queries.findGenerations(object);
    }

    @Override
    public Optional<RefreshAttempt> findAttempt(AttemptId attempt) {
        return queries.findAttempt(attempt);
    }

    @Override
    public Optional<RefreshAttempt> findActiveAttempt(MaterializationId object) {
        return queries.findActiveAttempt(object);
    }

    @Override
    public Optional<Generation> findGenerationByAttempt(AttemptId attempt) {
        return queries.findGenerationByAttempt(attempt);
    }

    @Override
    public List<RefreshAttempt> findReconcilableAttempts(Instant observedBefore) {
        return queries.findReconcilableAttempts(observedBefore);
    }

    @Override
    public void markAttemptBuilding(AttemptId attempt, Instant now) {
        sql.inTransaction("markAttemptBuilding", connection -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE materialization.attempts SET state='BUILDING', updated_at=? "
                            + "WHERE attempt_id=? AND state IN ('PREPARING','BUILDING')")) {
                setInstant(statement, 1, now);
                statement.setObject(2, attempt.value());
                if (statement.executeUpdate() != 1) {
                    throw MaterializationException.concurrentPublication(
                            "attempt " + attempt.value()
                                    + " is no longer preparing for execution");
                }
            }
            return null;
        });
    }

    @Override
    public Optional<Schedule> findSchedule(MaterializationId object) {
        return queries.findSchedule(object);
    }

    @Override
    public Optional<CommandRecord> findCommand(TenantId tenant, CommandId commandId) {
        return queries.findCommand(tenant, commandId);
    }

    @Override
    public Optional<MaterializationWriteCapability> findWriteCapability(AttemptId attempt) {
        return queries.findWriteCapability(attempt);
    }

    @Override
    public List<ReclaimableSourcePin> findReclaimableSourcePins(
            Instant now, Instant settledBefore, int attempts) {
        return queries.findReclaimableSourcePins(now, settledBefore, attempts);
    }

    @Override
    public List<ReaderPin> findReaderPins(GenerationId generation) {
        return queries.findReaderPins(generation);
    }

    @Override
    public List<Generation> findCollectableGenerations(Instant now, Instant eligibleBefore,
            int limit) {
        return queries.findCollectableGenerations(now, eligibleBefore, limit);
    }

    @Override
    public void recordCommitEvidence(AttemptId attemptId, CommitEvidence evidence, Instant now) {
        publications.recordCommitEvidence(attemptId, evidence, now);
    }

    @Override
    public PublicationResult publish(PublishCommand command, MaterializationEvent event) {
        return publications.publish(command, event);
    }

    @Override
    public void releaseSourcePins(AttemptId attempt, Instant now) {
        readerPins.releaseSourcePins(attempt, now);
    }

    @Override
    public void pinReader(ReaderPin pin) {
        readerPins.pinReader(pin);
    }

    @Override
    public void releaseReaderPin(GenerationId generation, QueryId query) {
        readerPins.releaseReaderPin(generation, query);
    }

    @Override
    public void releaseReaderPins(QueryId query) {
        readerPins.releaseReaderPins(query);
    }

    @Override
    public void renewReaderPins(QueryId query, Instant expiresAt, Instant heartbeatAt) {
        readerPins.renewReaderPins(query, expiresAt, heartbeatAt);
    }

    @Override
    public int purgeExpiredReaderPins(Instant now) {
        return readerPins.purgeExpiredReaderPins(now);
    }

    @Override
    public void recordGcDecision(GcLedgerEntry entry) {
        readerPins.recordGcDecision(entry);
    }

    @Override
    public void updateGcDecision(UUID entryId, GcLedgerEntry.GcState state, Instant now) {
        readerPins.updateGcDecision(entryId, state, now);
    }
}
