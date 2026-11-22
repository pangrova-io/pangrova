package org.pangrova.streaming.cdc;

import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.api.enums.CdcRowKind;
import org.pangrova.connector.api.data.ConnectorCdcPageSink;
import org.pangrova.connector.api.data.ConnectorPageSink;
import org.pangrova.connector.api.data.ConnectorPageSinkProvider;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.api.enums.CdcOperation;
import org.pangrova.types.value.TypedRow;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Stages CDC batches through the exact connector output handle bound to the task.
 *
 * <p>Connectors with CDC row-kind support stage all operations in one sink.
 * Connectors with append sinks can stage INSERT events. The Coordinator publishes
 * the resulting fragments through the journaled write path.</p>
 */
public final class BoundCdcPageSinkWriter implements CdcRangeBatchWriter {

    private final ConnectorPageSinkProvider sinkProvider;
    private final ConnectorSession session;
    private final ConnectorOutputTableHandle outputHandle;

    public BoundCdcPageSinkWriter(
            ConnectorPageSinkProvider sinkProvider,
            ConnectorSession session,
            ConnectorOutputTableHandle outputHandle) {
        this.sinkProvider = java.util.Objects.requireNonNull(sinkProvider, "sinkProvider");
        this.session = java.util.Objects.requireNonNull(session, "session");
        this.outputHandle = java.util.Objects.requireNonNull(outputHandle, "outputHandle");
    }

    @Override
    public List<byte[]> writeBatch(List<CdcEnvelope> envelopes) {
        if (envelopes.isEmpty() && !requiresSingleRange()) {
            return List.of();
        }
        ConnectorCdcPageSink sink;
        try {
            sink = sinkProvider.createCdcPageSink(
                    ConnectorTransactionHandle.empty(), session, outputHandle);
        } catch (UnsupportedOperationException noRowKindSink) {
            if (requiresSingleRange()
                    || envelopes.stream().anyMatch(envelope -> envelope.operation() != CdcOperation.INSERT)) {
                throw new UnsupportedOperationException(
                        "Connector does not support atomic CDC UPDATE/DELETE staging", noRowKindSink);
            }
            return List.of(stageInserts(envelopes));
        }
        try (sink) {
            try {
                for (CdcEnvelope envelope : envelopes) {
                    switch (envelope.operation()) {
                        case INSERT -> sink.appendRow(toTypedRow(requireRow(envelope.after(), "after")),
                                CdcRowKind.INSERT);
                        case DELETE -> sink.appendRow(toTypedRow(requireRow(envelope.before(), "before")),
                                CdcRowKind.DELETE);
                        case UPDATE -> {
                            sink.appendRow(toTypedRow(requireRow(envelope.before(), "before")),
                                    CdcRowKind.UPDATE_BEFORE);
                            sink.appendRow(toTypedRow(requireRow(envelope.after(), "after")),
                                    CdcRowKind.UPDATE_AFTER);
                        }
                        default -> throw new IllegalStateException(
                                "Unhandled CDC operation: " + envelope.operation());
                    }
                }
                return List.of(sink.finishWithMetadata());
            } catch (RuntimeException | Error failure) {
                try {
                    sink.abort();
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }
    }

    @Override
    public boolean requiresSingleRange() {
        return outputHandle.requiresSingleCdcRange();
    }

    private byte[] stageInserts(List<CdcEnvelope> envelopes) {
        List<CdcRow> rows = envelopes.stream().map(envelope -> requireRow(envelope.after(), "after")).toList();
        try (ConnectorPageSink sink = sinkProvider.createPageSink(
                ConnectorTransactionHandle.empty(), session, outputHandle)) {
            try {
                sink.appendPage(toPage(rows));
                return sink.finishWithMetadata();
            } catch (RuntimeException | Error failure) {
                try {
                    sink.abort();
                } catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }
    }

    private static CdcRow requireRow(CdcRow row, String image) {
        if (row == null) {
            throw new IllegalStateException("CDC envelope has no " + image + " image");
        }
        return row;
    }

    private static TypedRow toTypedRow(CdcRow row) {
        TypedRow.Builder builder = TypedRow.builder();
        for (String column : row.columnNames()) {
            builder.putRaw(column, row.get(column));
        }
        return builder.build();
    }

    private static Page toPage(List<CdcRow> rows) {
        List<String> columns = List.copyOf(new LinkedHashSet<>(rows.get(0).columnNames()));
        List<TypedRow> typed = new ArrayList<>(rows.size());
        for (CdcRow row : rows) {
            TypedRow.Builder builder = TypedRow.builder();
            for (String column : columns) {
                builder.putRaw(column, row.get(column));
            }
            typed.add(builder.build());
        }
        return new Page(columns, typed);
    }
}
