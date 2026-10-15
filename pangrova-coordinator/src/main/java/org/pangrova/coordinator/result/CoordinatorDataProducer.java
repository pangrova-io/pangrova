package org.pangrova.coordinator.result;

import org.pangrova.spi.transport.DataProducer;
import org.pangrova.spi.transport.DataProducerException;
import org.pangrova.spi.transport.DataTicket;
import org.pangrova.spi.transport.TransportStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Data producer serving bounded coordinator control-plane statement results.
 * Relational query rows are never written here; clients read those directly
 * from the Worker endpoint returned by dispatch.
 * Throws {@link DataProducerException} with {@link TransportStatus#NOT_FOUND}
 * when the queryId is missing or expired, so clients receive an explicit error
 * instead of a false-success empty result (EOF).
 */
public final class CoordinatorDataProducer implements DataProducer {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorDataProducer.class);

    private final CoordinatorResultBuffer buffer;

    public CoordinatorDataProducer(CoordinatorResultBuffer buffer) {
        this.buffer = Objects.requireNonNull(buffer, "buffer");
    }

    @Override
    public byte[] doGet(DataTicket ticket) {
        String queryId = ticket.queryId();
        byte[] data = buffer.get(queryId);
        if (data == null) {
            LOG.warn("event=result_not_found queryId={}", queryId);
            throw new DataProducerException(
                    TransportStatus.NOT_FOUND,
                    "Result not found for queryId=" + queryId);
        }
        LOG.debug("event=result_served queryId={} sizeBytes={}", queryId, data.length);
        return data;
    }

    @Override
    public boolean singleResponsePerRequest() {
        return true;
    }
}
