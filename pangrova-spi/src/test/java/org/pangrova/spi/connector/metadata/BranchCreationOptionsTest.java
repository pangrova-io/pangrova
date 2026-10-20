package org.pangrova.spi.connector.metadata;

import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BranchCreationOptionsTest {
    @Test
    void currentSourceDoesNotInventRetentionOrAnIdentifier() {
        var options = BranchCreationOptions.current();
        assertThat(options.sourceSnapshotId()).isEmpty();
        assertThat(options.sourceTag()).isEmpty();
        assertThat(options.maxReferenceAgeMillis()).isEmpty();
    }

    @Test
    void sourceIdentityAndExactLifetimeArePreserved() {
        var options = new BranchCreationOptions(OptionalLong.empty(), Optional.of("Release.中文\""),
                OptionalLong.of(604_800_000));
        assertThat(options.sourceTag()).contains("Release.中文\"");
        assertThat(options.maxReferenceAgeMillis()).hasValue(604_800_000);
    }

    @Test
    void ambiguousSourceAndNonpositiveValuesAreRejected() {
        assertThatThrownBy(() -> new BranchCreationOptions(OptionalLong.of(1), Optional.of("tag"),
                OptionalLong.empty())).isInstanceOf(IllegalArgumentException.class);
        for (long invalid : new long[] {0, -1, Long.MIN_VALUE}) {
            assertThatThrownBy(() -> new BranchCreationOptions(OptionalLong.of(invalid), Optional.empty(),
                    OptionalLong.empty())).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new BranchCreationOptions(OptionalLong.empty(), Optional.empty(),
                    OptionalLong.of(invalid))).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new BranchCreationOptions(OptionalLong.empty(), Optional.of(" "),
                OptionalLong.empty())).isInstanceOf(IllegalArgumentException.class);
    }
}
