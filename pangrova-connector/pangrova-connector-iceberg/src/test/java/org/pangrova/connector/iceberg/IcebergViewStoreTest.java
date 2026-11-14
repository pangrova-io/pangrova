package org.pangrova.connector.iceberg;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.enums.ViewCreateMode;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergViewStoreTest {
    private static final SchemaTableName NAME = new SchemaTableName("Mixed.Schema", "Mixed.View");

    @Test
    void creationModesNeverSilentlyReplaceDefinitions() {
        var store = new IcebergViewStore("catalog");
        store.create(NAME, "SELECT 1", ViewCreateMode.CREATE);
        assertThatThrownBy(() -> store.create(NAME, "SELECT 2", ViewCreateMode.CREATE))
                .isInstanceOfSatisfying(PangrovaException.class,
                        ex -> assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_TABLE_ALREADY_EXISTS));
        store.create(NAME, "SELECT 3", ViewCreateMode.IF_NOT_EXISTS);
        assertThat(store.definition(NAME)).contains("SELECT 1");
        store.create(NAME, "SELECT 4", ViewCreateMode.CREATE_OR_REPLACE);
        assertThat(store.definition(NAME)).contains("SELECT 4");
        store.drop(NAME, false);
        assertThat(store.definition(NAME)).isEmpty();
        assertThatThrownBy(() -> store.drop(NAME, false)).isInstanceOfSatisfying(PangrovaException.class,
                ex -> assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND));
        store.drop(NAME, true);
    }

    @Test
    void concurrentCreatorsHaveOneWinnerAndCannotOverwriteIt() throws Exception {
        var store = new IcebergViewStore("catalog");
        var barrier = new CyclicBarrier(2);
        var pool = Executors.newFixedThreadPool(2);
        try {
            var first = pool.submit(() -> createAfterBarrier(store, barrier, "SELECT 1"));
            var second = pool.submit(() -> createAfterBarrier(store, barrier, "SELECT 2"));
            String a = first.get(10, TimeUnit.SECONDS);
            String b = second.get(10, TimeUnit.SECONDS);
            assertThat(java.util.stream.Stream.of(a, b).filter(java.util.Objects::nonNull).toList()).hasSize(1);
            assertThat(store.definition(NAME)).contains(a == null ? b : a);
        } finally {
            pool.shutdownNow();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static String createAfterBarrier(IcebergViewStore store, CyclicBarrier barrier, String sql)
            throws Exception {
        barrier.await(10, TimeUnit.SECONDS);
        try {
            store.create(NAME, sql, ViewCreateMode.CREATE);
            return sql;
        } catch (PangrovaException conflict) {
            assertThat(conflict.getErrorCode()).isEqualTo(PangrovaErrorCode.CONNECTOR_TABLE_ALREADY_EXISTS);
            return null;
        }
    }
}
