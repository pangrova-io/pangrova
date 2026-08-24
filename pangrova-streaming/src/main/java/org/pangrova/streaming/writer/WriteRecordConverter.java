package org.pangrova.streaming.writer;

import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.pangrova.spi.streaming.CdcEnvelope;
import org.pangrova.spi.streaming.CdcOperation;
import org.pangrova.spi.streaming.CdcRow;

import java.util.Objects;

/**
 * Adapter converting CDC envelopes to SPI WriteRecord.
 *
 * <p>This conversion lives in the streaming module (not in SPI) because
 * it depends on CDC-specific types that must not leak into the SPI layer.</p>
 */
public final class WriteRecordConverter {

    private WriteRecordConverter() {}

    /** Convert a {@link CdcEnvelope} to a WriteRecord. */
    public static WriteRecord fromCdcEnvelope(CdcEnvelope envelope) {
        Objects.requireNonNull(envelope, "CDC envelope must not be null");
        WriteRecord.WriteOperation op = mapOperation(envelope.operation());
        TypedRow data = selectData(envelope);
        return new WriteRecord(data, op, envelope.eventTime());
    }

    private static WriteRecord.WriteOperation mapOperation(CdcOperation cdcOp) {
        return switch (cdcOp) {
            case INSERT -> WriteRecord.WriteOperation.INSERT;
            case UPDATE -> WriteRecord.WriteOperation.UPDATE;
            case DELETE -> WriteRecord.WriteOperation.DELETE;
        };
    }

    private static TypedRow selectData(CdcEnvelope envelope) {
        CdcRow row = envelope.operation() == CdcOperation.DELETE
            ? envelope.before() : envelope.after();
        return row == null ? null : TypedRow.fromRawMap(row.toMap());
    }
}
