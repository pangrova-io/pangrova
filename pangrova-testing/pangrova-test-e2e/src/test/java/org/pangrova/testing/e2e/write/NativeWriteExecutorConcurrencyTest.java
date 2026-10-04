package org.pangrova.testing.e2e.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;
import org.pangrova.worker.jni.write.WriteCommitResult;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Concurrent production claims and real HTTP/file ownership, without replacement collaborators. */
class NativeWriteExecutorConcurrencyTest extends NativeWriteLifecycleTestBase {
    @Test
    void actualBlockedDeploymentCannotMakeCloseWaitForIoOrLoseItsInitializingOwner() throws Exception {
        String table = newTable();
        String coordinatorUrl = "http://localhost:" + coordinatorHttpServer.getPort();
        try (var transport = new CatalogDeploymentTransport(coordinatorUrl);
                var fixture = lifecycle(transport.url())) {
            var executor = Executors.newFixedThreadPool(2);
            try {
                var opening = executor.submit(() -> fixture.open(table));
                assertThat(transport.awaitRequest()).isTrue();
                var closing = executor.submit(() -> assertThatThrownBy(fixture.executor()::close)
                        .isInstanceOf(IllegalStateException.class));
                closing.get(2, TimeUnit.SECONDS);
                assertThat(fixture.cachedCatalogRevisionCount()).isEqualTo(1);
                assertThat(fixture.executor().activeSessionCount()).isZero();
                transport.release();
                assertThatThrownBy(() -> opening.get(10, TimeUnit.SECONDS))
                        .hasRootCauseInstanceOf(IllegalStateException.class);
            } finally {
                transport.release();
                executor.shutdown();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
            fixture.close();
            assertThat(fixture.cachedCatalogRevisionCount()).isZero();
            assertThat(fixture.executor().activeSessionCount()).isZero();
        }
    }

    @Test
    void differentTasksOpenAppendAndFinishWithDistinctRealFilesAndExactRows() throws Exception {
        String firstTable = newTable();
        String secondTable = newTable();
        try (var fixture = lifecycle()) {
            var executor = Executors.newFixedThreadPool(2);
            CountDownLatch start = new CountDownLatch(1);
            try {
                var first = executor.submit(() -> {
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    return fixture.open(firstTable);
                });
                var second = executor.submit(() -> {
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    return fixture.open(secondTable);
                });
                start.countDown();
                var firstOwner = first.get(10, TimeUnit.SECONDS);
                var secondOwner = second.get(10, TimeUnit.SECONDS);
                assertThat(firstOwner.handle()).isNotEqualTo(secondOwner.handle());
                assertThat(firstOwner.taskId()).isNotEqualTo(secondOwner.taskId());
                assertThat(fixture.executor().activeSessionCount()).isEqualTo(2);
                var firstWrite = executor.submit(() -> prepare(fixture, firstOwner, 11));
                var secondWrite = executor.submit(() -> prepare(fixture, secondOwner, 22));
                assertPrepared(fixture, firstOwner, firstWrite.get(10, TimeUnit.SECONDS), 11);
                assertPrepared(fixture, secondOwner, secondWrite.get(10, TimeUnit.SECONDS), 22);
                assertThat(fixture.executor().activeSessionCount()).isZero();
            } finally {
                start.countDown();
                executor.shutdown();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static PreparedWrite prepare(NativeWriteLifecycleFixture fixture,
            NativeWriteLifecycleFixture.OpenSession owner, long value) throws Exception {
        Path file = fixture.appendAndFindOwnedFile(owner, value);
        return new PreparedWrite(file, fixture.executor().commitWrite(owner.handle()));
    }

    private static void assertPrepared(NativeWriteLifecycleFixture fixture,
            NativeWriteLifecycleFixture.OpenSession owner, PreparedWrite prepared, long value) throws Exception {
        assertThat(prepared.result().rowsWritten()).isEqualTo(1);
        assertThat(prepared.result().commitMetadata()).isNotEmpty();
        assertThat(fixture.executor().drainCommitMetadataForTask(owner.taskId()))
                .isEqualTo(prepared.result().commitMetadata());
        var schema = SchemaParser.fromJson(owner.outputHandle().schemaJson());
        var input = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(prepared.file().toUri()),
                new Configuration());
        try (CloseableIterable<Record> rows = Parquet.read(input).project(schema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema)).build()) {
            List<Long> actual = StreamSupport.stream(rows.spliterator(), false)
                    .map(row -> ((Number) row.getField("id")).longValue()).toList();
            assertThat(actual).containsExactly(value);
        }
    }

    private record PreparedWrite(Path file, WriteCommitResult result) {}
}
