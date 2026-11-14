package org.pangrova.connector.api.data.write;

import org.pangrova.connector.api.write.TableWriter;

import org.pangrova.connector.api.data.ConnectorPageSink;
import org.pangrova.connector.api.write.WriteContext;
import org.pangrova.connector.api.enums.WriteMode;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract tests for the write SPI hierarchy.
 *
 * <p>Verifies the design decision documented in F-SPI-01:
 * <ul>
 *   <li>{@link ConnectorPageSink} is the canonical connector-level write SPI with
 *       two-phase commit (appendPage / finishWithMetadata / abort / close)</li>
 *   <li>{@link TableWriter} is the streaming write contract with single-shot
 *       write semantics (no commit/abort needed)</li>
 *   <li>There is no {@code WriteContext} transaction mode concept -- streaming
 *       writes are always self-managed</li>
 *   <li>{@code DataFileSink} was removed from the SPI (it was a redundant third
 *       write interface competing with ConnectorPageSink and TableWriter); it now
 *       lives in the streaming module as an internal implementation detail</li>
 * </ul>
 */
class WriteSpiContractTest {

    @Test
    void connectorPageSinkHasTwoPhaseCommitMethods() {
        Set<String> methods = methodNames(ConnectorPageSink.class);
        assertTrue(methods.contains("appendPage"),
                "ConnectorPageSink must have appendPage for data ingestion");
        assertTrue(methods.contains("finishWithMetadata"),
                "ConnectorPageSink must have finishWithMetadata for commit metadata");
        assertTrue(methods.contains("abort"),
                "ConnectorPageSink must have abort for failure path");
        assertTrue(methods.contains("close"),
                "ConnectorPageSink must have close for resource cleanup");
    }

    @Test
    void tableWriterHasSingleShotWriteSemantics() {
        Set<String> methods = methodNames(TableWriter.class);
        assertTrue(methods.contains("write"),
                "TableWriter must have write for single-shot streaming writes");
        assertFalse(methods.contains("commit"),
                "TableWriter must not have commit (streaming writes are self-managed)");
        assertFalse(methods.contains("abort"),
                "TableWriter must not have abort (streaming writes are self-managed)");
        assertFalse(methods.contains("finishWithMetadata"),
                "TableWriter must not have finishWithMetadata (not a 2PC contract)");
    }

    @Test
    void tableWriterWriteMethodAcceptsWriteContext() {
        Method writeMethod = Arrays.stream(TableWriter.class.getDeclaredMethods())
                .filter(m -> m.getName().equals("write"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("write method not found"));

        Class<?>[] paramTypes = writeMethod.getParameterTypes();
        assertEquals(4, paramTypes.length,
                "write() should take 4 parameters: tableName, records, writeMode, context");
        assertEquals(String.class, paramTypes[0], "First param should be tableName (String)");
        assertEquals(List.class, paramTypes[1], "Second param should be records (List)");
        assertEquals(WriteMode.class, paramTypes[2], "Third param should be writeMode (WriteMode)");
        assertEquals(WriteContext.class, paramTypes[3], "Fourth param should be context (WriteContext)");
    }

    @Test
    void streamWriteSinkRemovedAsDuplicate() {
        // StreamWriteSink was a dead duplicate of CdcWriteSink (same contract:
        // writeBatch(List<CdcEnvelope>) -> result). Unified into CdcWriteSink
        // per Task 211: one self-consistent write contract.
        try {
            Class.forName("org.pangrova.streaming.api.cdc.StreamWriteSink");
            fail("StreamWriteSink should have been removed "
                    + "(Task 211: dead duplicate of CdcWriteSink)");
        } catch (ClassNotFoundException expected) {
            // expected: StreamWriteSink deleted
        }
    }

    @Test
    void streamWriteResultRemovedAsDuplicate() {
        try {
            Class.forName("org.pangrova.streaming.api.cdc.StreamWriteResult");
            fail("StreamWriteResult should have been removed "
                    + "(Task 211: dead duplicate of CdcWriteResult)");
        } catch (ClassNotFoundException expected) {
            // expected: StreamWriteResult deleted
        }
    }

    @Test
    void connectorPageSinkAndTableWriterAreDistinctHierarchies() {
        assertFalse(ConnectorPageSink.class.isAssignableFrom(TableWriter.class),
                "TableWriter must not extend ConnectorPageSink");
        assertFalse(TableWriter.class.isAssignableFrom(ConnectorPageSink.class),
                "ConnectorPageSink must not extend TableWriter");
    }

    @Test
    void tableWriterIsAutoCloseable() {
        assertTrue(AutoCloseable.class.isAssignableFrom(TableWriter.class),
                "TableWriter must be AutoCloseable for resource management");
    }

    private static Set<String> methodNames(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredMethods())
                .map(Method::getName)
                .collect(Collectors.toSet());
    }
}
