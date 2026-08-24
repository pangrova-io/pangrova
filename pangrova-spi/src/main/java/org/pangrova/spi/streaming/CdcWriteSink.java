package org.pangrova.spi.streaming;

import org.pangrova.spi.annotation.Experimental;

import java.util.List;

/**
 * SPI for writing CDC data to a target table.
 *
 * <p>This interface is part of the <em>declarative incremental data transformation</em>
 * subsystem. It writes parsed CDC change events to lake tables for incremental ETL
 * and materialized view refresh. It is NOT a general-purpose stream processing sink.</p>
 *
 * <p>The coordinator provides an implementation backed by {@code ConnectorPageSinkProvider},
 * which goes through the standard connector write path. This ensures:</p>
 * <ul>
 *   <li>Index construction is triggered on commit</li>
 *   <li>Compaction scheduling is triggered on write completion</li>
 *   <li>Connector-level transaction semantics (Iceberg commit / Paimon commit) are honored</li>
 *   <li>Clustering, schema validation, and all other connector-level hooks fire normally</li>
 * </ul>
 *
 * <p>Implementations must be thread-safe if shared across pipelines.</p>
 */
@Experimental
public interface CdcWriteSink extends AutoCloseable {

    /**
     * Write a batch of CDC envelopes to the target table.
     *
     * @param envelopes parsed CDC change events, must not be null or empty
     * @return write result with metadata (rows written, bytes, data files produced)
     */
    CdcWriteResult writeBatch(List<CdcEnvelope> envelopes);

    /**
     * A-EOS-02: opaque token for a batch that has been STAGED (its data written to storage) but not
     * yet published to the catalog. Produced by {@link #stage}, consumed by {@link #publish} or
     * {@link #discard}.
     */
    interface StagedWrite { }

    /**
     * A-EOS-02 (2PC prepare phase): write the batch's data to storage WITHOUT publishing it to the
     * catalog, returning a token to later {@link #publish} or {@link #discard}. This makes the
     * streaming write a true two-phase commit (stage → publish/discard) rather than effectively-once.
     *
     * <p>Default: buffers the envelopes in memory (no durable staging). Connector-backed sinks
     * override to write real staged files so a crash between prepare and commit is recoverable via
     * {@link #discard}.
     */
    default StagedWrite stage(List<CdcEnvelope> envelopes) {
        return new BufferedStage(envelopes);
    }

    /**
     * A-EOS-02 (2PC commit phase): atomically publish previously {@link #stage}d data to the
     * catalog. Must be idempotent (safe to re-invoke on crash-recovery replay).
     */
    default CdcWriteResult publish(StagedWrite staged) {
        if (!(staged instanceof BufferedStage buffered)) {
            throw new IllegalStateException("Default publish() only supports the buffered default "
                    + "staging; a sink that overrides stage() must also override publish(). Got: "
                    + (staged == null ? "null" : staged.getClass().getName()));
        }
        return writeBatch(buffered.envelopes);
    }

    /**
     * A-EOS-02 (2PC abort phase): discard staged-but-unpublished data (delete staged files).
     * Default is a no-op because the buffered default staging wrote nothing durable.
     */
    default void discard(StagedWrite staged) {
        // Buffered default staging wrote nothing durable; nothing to discard.
    }

    @Override
    void close();

    /** In-memory staging token for the buffered {@link #stage} default (no durable staging). */
    final class BufferedStage implements StagedWrite {
        final List<CdcEnvelope> envelopes;

        BufferedStage(List<CdcEnvelope> envelopes) {
            this.envelopes = envelopes;
        }
    }
}
