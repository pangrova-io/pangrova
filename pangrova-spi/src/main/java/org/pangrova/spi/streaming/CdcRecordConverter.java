package org.pangrova.spi.streaming;

import org.pangrova.spi.annotation.Experimental;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * SPI for deserializing raw source records into CDC envelopes.
 *
 * <p>Implementations handle format-specific parsing (Debezium JSON, Canal JSON,
 * Maxwell JSON, Avro, etc.) and produce a uniform {@link CdcEnvelope} output.
 * The pipeline selects the converter based on the FORMAT clause in CREATE STREAM.</p>
 *
 * <p>Implementations must be stateless and thread-safe.</p>
 */
@Experimental
public interface CdcRecordConverter {

    /**
     * Unique format identifier (e.g., "debezium-json", "canal-json", "maxwell-json").
     *
     * @return non-null format identifier
     */
    String formatId();

    /**
     * Deserialize a raw byte payload into a CdcEnvelope.
     *
     * @param payload raw message bytes from the source
     * @return parsed CDC envelope, never null
     * @throws CdcRecordConversionException if the payload cannot be parsed
     */
    CdcEnvelope deserialize(byte[] payload);

    /**
     * Deserialize a batch of raw payloads, returning successfully parsed envelopes
     * and routing failures to the dead-letter callback.
     *
     * <p>The default implementation iterates over each payload, calling
     * {@link #deserialize(byte[])} and routing any exception to the dead-letter
     * callback. Implementations may override for batch-optimized parsing.</p>
     *
     * @param payloads   raw message bytes
     * @param deadLetter callback for unparseable records (payload, exception)
     * @return list of successfully parsed envelopes
     */
    default List<CdcEnvelope> convertBatch(List<byte[]> payloads,
                                           BiConsumer<byte[], Exception> deadLetter) {
        List<CdcEnvelope> results = new ArrayList<>();
        for (byte[] payload : payloads) {
            try {
                results.add(deserialize(payload));
            } catch (Exception e) {
                deadLetter.accept(payload, e);
            }
        }
        return results;
    }
}
