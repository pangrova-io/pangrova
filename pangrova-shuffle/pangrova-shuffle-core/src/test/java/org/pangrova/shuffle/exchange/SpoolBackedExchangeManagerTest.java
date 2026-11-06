package org.pangrova.shuffle.exchange;

import org.pangrova.shuffle.api.identity.ExchangeChannelContext;

import org.pangrova.shuffle.api.identity.ExchangeAttemptFence;

import org.pangrova.shuffle.api.identity.ExchangeTaskAttempt;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.batch.api.page.BatchFormat;
import org.pangrova.batch.api.page.NativePage;
import org.pangrova.shuffle.api.ShuffleExecutionOwner;
import org.pangrova.shuffle.api.spool.SpoolReadSelection;
import org.pangrova.shuffle.local.spool.LocalFsSpoolStore;

import static org.assertj.core.api.Assertions.*;

class SpoolBackedExchangeManagerTest {
    @TempDir Path directory;

    @Test
    void realStoreRoundTripPreservesPagesAndSelectedAttempt() throws Exception {
        var store = new LocalFsSpoolStore(directory);
        try (var manager = new SpoolBackedExchangeManager(store,
                context -> new SpoolReadSelection("q", 0, 0, Map.of(0, 0L)))) {
            try (var selected = manager.createSink(context(0));
                 var first = page("first", BatchFormat.ARROW_IPC);
                 var second = page("second", BatchFormat.ARROW_IPC)) {
                selected.addNativePage(first);
                selected.addNativePage(second);
                selected.finish();
                selected.finish();
            }
            try (var loser = manager.createSink(context(1)); var value = page("loser", BatchFormat.ARROW_IPC)) {
                loser.addNativePage(value);
                loser.finish();
            }
            try (var source = manager.createSource(context(0), ExchangeReadMode.BLOCKING);
                 var first = source.getNextNativePage(); var second = source.getNextNativePage()) {
                assertThat(new String(first.serializedBatch(), StandardCharsets.UTF_8)).isEqualTo("first");
                assertThat(new String(second.serializedBatch(), StandardCharsets.UTF_8)).isEqualTo("second");
                assertThat(first.format()).isEqualTo(BatchFormat.ARROW_IPC);
                assertThat(source.getNextNativePage()).isNull();
                assertThat(source.isFinished()).isTrue();
                assertThatThrownBy(() -> store.delete("q")).isInstanceOf(IllegalStateException.class);
            }
            store.delete("q");
        }
    }

    @Test
    void emptyFinishedSinkHasExplicitReadableCompletion() throws Exception {
        try (var manager = manager()) {
            try (var sink = manager.createSink(context(0))) { sink.finish(); }
            try (var source = manager.createSource(context(0), ExchangeReadMode.BLOCKING)) {
                assertThat(source.getNextNativePage()).isNull();
                assertThat(source.isFinished()).isTrue();
            }
        }
    }

    @Test
    void missingSelectionFailsRepeatedReadsInsteadOfBecomingSuccessfulEof() throws Exception {
        try (var manager = manager(); var source = manager.createSource(context(0), ExchangeReadMode.BLOCKING)) {
            assertThatThrownBy(source::getNextNativePage).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(source::getNextNativePage).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void abortOrCloseBeforeFinishCannotLaterCompleteAnEmptyAttempt() throws Exception {
        try (var manager = manager()) {
            var sink = manager.createSink(context(0));
            sink.abort();
            assertThatThrownBy(sink::finish).isInstanceOf(IllegalStateException.class);
            var closed = manager.createSink(context(1));
            closed.close();
            assertThatThrownBy(closed::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void rejectedWriterDoesNotTurnIntoSuccessfulFinish() throws Exception {
        try (var manager = manager(); var first = manager.createSink(context(0));
             var duplicate = manager.createSink(context(0)); var data = page("data", BatchFormat.ARROW_IPC)) {
            first.addNativePage(data);
            assertThatThrownBy(() -> duplicate.addNativePage(data)).isInstanceOf(IllegalStateException.class);
            first.finish();
            assertThatThrownBy(duplicate::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void unsupportedFormatAndStreamingModeAreNotSilentlyReinterpreted() throws Exception {
        try (var manager = manager(); var sink = manager.createSink(context(0));
             var custom = page("custom", BatchFormat.CUSTOM_BINARY)) {
            assertThatThrownBy(() -> sink.addNativePage(custom)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> manager.createSource(context(0), ExchangeReadMode.STREAMING))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void foreignQueryStageOrPartitionSelectionsAreRejected() throws Exception {
        for (var selection : java.util.List.of(new SpoolReadSelection("other", 0, 0, Map.of()),
                new SpoolReadSelection("q", 1, 0, Map.of()), new SpoolReadSelection("q", 0, 1, Map.of()))) {
            try (var manager = new SpoolBackedExchangeManager(new LocalFsSpoolStore(directory), ignored -> selection)) {
                assertThatThrownBy(() -> manager.createSource(context(0), ExchangeReadMode.BLOCKING))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @Test
    void closedSourceDoesNotLazilyAcquireAReaderAfterClose() throws Exception {
        var store = new LocalFsSpoolStore(directory);
        try (var manager = new SpoolBackedExchangeManager(store,
                ignored -> new SpoolReadSelection("q", 0, 0, Map.of(0, 0L)))) {
            try (var sink = manager.createSink(context(0))) { sink.finish(); }
            var source = manager.createSource(context(0), ExchangeReadMode.BLOCKING);
            source.close();
            assertThat(source.getNextNativePage()).isNull();
            store.delete("q");
        }
    }

    private SpoolBackedExchangeManager manager() {
        return new SpoolBackedExchangeManager(new LocalFsSpoolStore(directory),
                ignored -> new SpoolReadSelection("q", 0, 0, Map.of(0, 0L)));
    }
    private static NativePage page(String text, BatchFormat format) {
        return new NativePage(text.getBytes(StandardCharsets.UTF_8), -1, -1, format);
    }
    private static ExchangeChannelContext context(int attempt) {
        return new ExchangeChannelContext(new ExchangeTaskAttempt("q", 0, 0, "task-" + attempt, 1,
                new ExchangeAttemptFence(1, attempt, 1), new ShuffleExecutionOwner("owner", "incarnation", 1),
                UUID.randomUUID().toString(), UUID.randomUUID().toString()), 0, Long.MAX_VALUE);
    }
}
