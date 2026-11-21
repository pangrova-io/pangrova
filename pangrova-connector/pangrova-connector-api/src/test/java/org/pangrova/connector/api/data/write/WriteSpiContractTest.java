package org.pangrova.connector.api.data.write;

import org.pangrova.connector.api.data.ConnectorPageSink;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract tests for the connector write SPI hierarchy.
 *
 * <p>{@link ConnectorPageSink} is the canonical connector-level write SPI with
 * two-phase commit (appendPage / finishWithMetadata / abort / close). The former
 * single-shot batch-write hierarchy had no production factory, consumer or
 * close owner: the CDC runtime writes through CdcWriteSink and
 * ConnectorPageSinkCdcWriter, and SQL writes use the same connector sinks.</p>
 */
class WriteSpiContractTest {

    private static final List<String> RETIRED_BATCH_WRITE_TYPES = List.of(
            "org.pangrova.connector.api.write.TableWriter",
            "org.pangrova.connector.api.write.WriteRecord",
            "org.pangrova.connector.api.write.WriteContext",
            "org.pangrova.connector.api.write.DataFileSink",
            "org.pangrova.connector.api.enums.WriteMode",
            "org.pangrova.connector.api.enums.TableFormat",
            "org.pangrova.streaming.writer.lake.LakeTableWriter",
            "org.pangrova.streaming.writer.mergetree.MergeTreeTableWriter",
            "org.pangrova.streaming.writer.incremental.IncrementalWriter");

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
    void retiredBatchWriteHierarchyStaysRemoved() {
        for (String type : RETIRED_BATCH_WRITE_TYPES) {
            try {
                Class.forName(type);
                fail(type + " had no production constructor, factory or consumer; it must stay retired");
            } catch (ClassNotFoundException expected) {
                // expected: the dead batch-write hierarchy was retired
            }
        }
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

    private static Set<String> methodNames(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredMethods())
                .map(Method::getName)
                .collect(Collectors.toSet());
    }
}
