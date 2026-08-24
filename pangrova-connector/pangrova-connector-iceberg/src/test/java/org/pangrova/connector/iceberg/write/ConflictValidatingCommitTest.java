package org.pangrova.connector.iceberg.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.junit.jupiter.api.Test;

/**
 * Verifies the SI-2 commit chokepoint (G-DML-01 / G-CONN-01 / F-ICEBERG-01/04): OCC validations are
 * attached unconditionally when a base snapshot is present, and skipped only for append-like writes
 * with no base (negative sentinel). Uses a JDK dynamic proxy (no mock framework on the connector test
 * classpath) that records method calls and returns itself for fluent chaining.
 */
class ConflictValidatingCommitTest {

    /** Records invoked method names; fluent methods return the proxy for chaining. */
    @SuppressWarnings("unchecked")
    private static <T> T recordingBuilder(Class<T> iface, List<String> calls) {
        Object proxy = Proxy.newProxyInstance(
                iface.getClassLoader(),
                new Class<?>[] {iface},
                (p, method, args) -> {
                    calls.add(method.getName());
                    // Fluent builder methods return the same builder type → return the proxy.
                    if (iface.isAssignableFrom(method.getReturnType())) {
                        return p;
                    }
                    return null;
                });
        return iface.cast(proxy);
    }

    @Test
    void replacePartitionsValidatesAgainstBaseSnapshot() {
        List<String> calls = new ArrayList<>();
        ReplacePartitions update = recordingBuilder(ReplacePartitions.class, calls);
        ReplacePartitions returned = ConflictValidatingCommit.validate(update, 12345L);
        assertThat(returned).isNotNull();
        assertThat(calls).containsExactly(
                "validateFromSnapshot", "validateNoConflictingData", "validateNoConflictingDeletes");
    }

    @Test
    void rowDeltaValidatesAgainstBaseSnapshot() {
        List<String> calls = new ArrayList<>();
        RowDelta update = recordingBuilder(RowDelta.class, calls);
        ConflictValidatingCommit.validate(update, 12345L);
        assertThat(calls).containsExactly("validateFromSnapshot");
    }

    @Test
    void rowDeltaValidatesReferencedDataFilesExistWhenManifestPresent() {
        List<String> calls = new ArrayList<>();
        RowDelta update = recordingBuilder(RowDelta.class, calls);
        ConflictValidatingCommit.validate(update, 12345L, List.of("/warehouse/data/f.parquet"));
        assertThat(calls).containsExactly(
                "validateFromSnapshot", "validateDataFilesExist", "validateDeletedFiles");
    }

    @Test
    void rowDeltaSkipsDataFileExistenceWhenManifestEmpty() {
        List<String> calls = new ArrayList<>();
        RowDelta update = recordingBuilder(RowDelta.class, calls);
        ConflictValidatingCommit.validate(update, 12345L, List.of());
        assertThat(calls).containsExactly("validateFromSnapshot");
    }

    @Test
    void noValidationWhenNoBaseSnapshot() {
        List<String> calls = new ArrayList<>();
        ReplacePartitions rp = recordingBuilder(ReplacePartitions.class, calls);
        assertThat(ConflictValidatingCommit.validate(rp, -1L)).isSameAs(rp);
        RowDelta rd = recordingBuilder(RowDelta.class, calls);
        assertThat(ConflictValidatingCommit.validate(rd, -1L)).isSameAs(rd);
        assertThat(calls).isEmpty();
    }
}
