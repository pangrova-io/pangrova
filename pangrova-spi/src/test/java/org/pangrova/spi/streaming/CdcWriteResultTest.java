package org.pangrova.spi.streaming;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link CdcWriteResult} compact constructor validation.
 */
class CdcWriteResultTest {

    @Test
    void validResultIsConstructed() {
        CdcWriteResult result = new CdcWriteResult(10, 1024, 1, "catalog.schema.orders");

        assertEquals(10, result.rowsWritten());
        assertEquals(1024, result.bytesWritten());
        assertEquals(1, result.dataFilesProduced());
        assertEquals("catalog.schema.orders", result.targetTable());
    }

    @Test
    void zeroValuesAreAccepted() {
        CdcWriteResult result = new CdcWriteResult(0, 0, 0, "catalog.schema.t");

        assertEquals(0, result.rowsWritten());
        assertEquals(0, result.bytesWritten());
        assertEquals(0, result.dataFilesProduced());
    }

    @Test
    void negativeRowsWrittenIsRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new CdcWriteResult(-1, 0, 0, "t"));
        assertEquals("rowsWritten must be non-negative, got: -1", ex.getMessage());
    }

    @Test
    void negativeBytesWrittenIsRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new CdcWriteResult(0, -1, 0, "t"));
        assertEquals("bytesWritten must be non-negative, got: -1", ex.getMessage());
    }

    @Test
    void negativeDataFilesProducedIsRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new CdcWriteResult(0, 0, -1, "t"));
        assertEquals("dataFilesProduced must be non-negative, got: -1", ex.getMessage());
    }

    @Test
    void nullTargetTableIsRejected() {
        assertThrows(NullPointerException.class,
                () -> new CdcWriteResult(0, 0, 0, null));
    }
}
