package org.pangrova.worker.copy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.StageResourceGrant;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CopyPreparationTest {
    @TempDir Path directory;

    @Test
    void wholePipelinePreflightRejectsBeforeConsumingAnyAllowance() {
        var budget = budget(9, 5, 5);
        assertThatThrownBy(() -> budget.requireStageCapacity(5)).isInstanceOf(CopyPreparation.Halted.class);
        assertThat(budget.usage().getObjectStoreReadBytes()).isZero();
        assertThat(budget.usage().getObjectStoreWriteBytes()).isZero();
        assertThat(budget.usage().getSpillBytes()).isZero();
        assertThatThrownBy(() -> budget.requireStageCapacity(Long.MAX_VALUE))
                .isInstanceOf(CopyPreparation.Halted.class);
    }

    @Test
    void enteredReadWriteAndDraftReservationsAreCumulativeAndCannotBeRefunded() {
        var budget = budget(10, 5, 5);
        budget.requireStageCapacity(5);
        budget.reserveRead(5);
        budget.reserveRead(5);
        budget.reserveWrite(5);
        budget.reserveDraft(5);
        assertThatThrownBy(() -> budget.reserveRead(1)).isInstanceOf(CopyPreparation.Halted.class);
        assertThatThrownBy(() -> budget.reserveWrite(1)).isInstanceOf(CopyPreparation.Halted.class);
        assertThatThrownBy(() -> budget.reserveDraft(1)).isInstanceOf(CopyPreparation.Halted.class);
        assertThat(budget.usage().getObjectStoreReadBytes()).isEqualTo(10);
        assertThat(budget.usage().getObjectStoreWriteBytes()).isEqualTo(5);
        assertThat(budget.usage().getSpillBytes()).isEqualTo(5);
    }

    @Test
    void cancellationPreventsFurtherIoButAllowsRealCleanup() throws Exception {
        var budget = budget(10, 5, 5);
        Path draft = Files.createFile(directory.resolve("cancelled"));
        budget.ownDraft(draft);
        budget.reserveRead(5);
        budget.cancel();
        assertThatThrownBy(() -> budget.reserveRead(1)).isInstanceOfSatisfying(CopyPreparation.Halted.class,
                halted -> assertThat(halted.category()).isEqualTo(ErrorClass.ERROR_CLASS_CANCELLED));
        assertThat(budget.cleanupResources()).isTrue();
        assertThat(budget.resourcesReleased()).isTrue();
        assertThat(draft).doesNotExist();
        assertThat(budget.usage().getObjectStoreReadBytes()).isEqualTo(5);
    }

    @Test
    void failedClientCloseRetainsOwnerWhileStillAttemptingIndependentDraftCleanup() throws Exception {
        var budget = budget(10, 5, 5);
        Path draft = Files.createFile(directory.resolve("cleanup"));
        var attempts = new AtomicInteger();
        budget.ownDraft(draft);
        budget.ownSourceClient(() -> {
            if (attempts.incrementAndGet() == 1) throw new IOException("close failed");
        });
        assertThatThrownBy(budget::cleanupResources).isInstanceOf(IOException.class);
        assertThat(draft).doesNotExist();
        assertThat(budget.resourcesReleased()).isFalse();
        assertThat(budget.cleanupResources()).isTrue();
        assertThat(attempts).hasValue(2);
        assertThat(budget.resourcesReleased()).isTrue();
    }

    private static CopyPreparation budget(long reads, long writes, long spill) {
        return new CopyPreparation(StageResourceGrant.newBuilder().setJvmHeapBytes(1024 * 1024)
                .setObjectStoreReadBytes(reads).setObjectStoreWriteBytes(writes).setSpillBytes(spill).build(),
                2_000, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    }
}
