package org.pangrova.streaming.watermark;

import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Routes late-arriving records to side-output table instead of dropping them.
 */
public class LateDataRouter {

    private static final Logger LOG = LoggerFactory.getLogger(LateDataRouter.class);

    private final WatermarkTracker watermarkTracker;
    private final TableWriter sideOutputWriter;
    private final LateDataConfig config;
    private final AtomicLong lateDataTotal = new AtomicLong(0);

    public LateDataRouter(WatermarkTracker watermarkTracker,
                          TableWriter sideOutputWriter, LateDataConfig config) {
        this.watermarkTracker = watermarkTracker;
        this.sideOutputWriter = sideOutputWriter;
        this.config = config;
    }

    /**
     * Route record to main stream or side-output based on watermark.
     * @return true if routed to main stream, false if to side-output
     */
    public boolean route(String tableName, WriteRecord record) {
        if (!config.enabled()) return true;
        Instant watermark = watermarkTracker.getWatermark(tableName);
        if (!record.eventTime().isBefore(watermark)) return true;
        routeToSideOutput(tableName, record, watermark);
        return false;
    }

    public long lateDataTotal() {
        return lateDataTotal.get();
    }

    private void routeToSideOutput(String tableName, WriteRecord record,
                                   Instant watermark) {
        String sideTable = tableName + config.sideOutputTableSuffix();
        TypedRow enriched = enrichWithMetadata(record, watermark);
        WriteRecord sideRecord = new WriteRecord(
            enriched, record.operation(), record.eventTime());
        try {
            sideOutputWriter.write(sideTable, List.of(sideRecord),
                WriteMode.APPEND, WriteContext.selfManaged());
        } catch (Exception e) {
            LOG.error("side_output_write_failed table={} sideTable={} error={}",
                tableName, sideTable, e.getMessage());
            throw new LateDataRouteException(
                "Failed to write late data to side-output table: " + sideTable, e);
        }
        lateDataTotal.incrementAndGet();
    }

    private TypedRow enrichWithMetadata(WriteRecord record, Instant watermark) {
        return record.data()
            .withColumn("_arrival_time",
                new ColumnValue.StringValue(Instant.now().toString()))
            .withColumn("_watermark_at_arrival",
                new ColumnValue.StringValue(watermark.toString()));
    }
}
