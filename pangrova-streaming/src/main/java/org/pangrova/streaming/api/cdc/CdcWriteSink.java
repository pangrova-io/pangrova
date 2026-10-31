package org.pangrova.streaming.api.cdc;

import org.pangrova.common.api.annotation.Experimental;

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

    /** Opaque token produced by {@link #stage} and consumed by {@link #publish} or {@link #discard}. */
    interface StagedWrite { }

    /**
     * Prepares a batch without publishing it to the catalog. Implementations must document
     * whether the returned token is durable and recoverable after a process crash.
     *
     * <p>The default only buffers envelopes in memory and is not durable. Callers must not
     * infer exactly-once delivery from this interface alone.
     */
    default StagedWrite stage(List<CdcEnvelope> envelopes) {
        return new BufferedStage(envelopes);
    }

    /** Publishes a prepared batch. Atomicity and replay safety depend on the implementation. */
    default CdcWriteResult publish(StagedWrite staged) {
        if (!(staged instanceof BufferedStage buffered)) {
            throw new IllegalStateException("Default publish() only supports the buffered default "
                    + "staging; a sink that overrides stage() must also override publish(). Got: "
                    + (staged == null ? "null" : staged.getClass().getName()));
        }
        return writeBatch(buffered.envelopes);
    }

    /** Discards an unpublished batch. The buffered default wrote nothing durable. */
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
