package org.pangrova.metadata.core.catalog;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogDivergenceReportTest {
    @Test
    void everyPublicConstructionOwnsAnImmutableReconciliationSnapshot() {
        var loaded = new ArrayList<>(List.of("loaded"));
        var removed = new ArrayList<>(List.of("removed"));
        var failed = new ArrayList<>(List.of("failed"));
        var report = new CatalogRegistry.DivergenceReport(loaded, removed, failed);
        loaded.clear();
        removed.clear();
        failed.clear();

        assertThat(report.loaded()).containsExactly("loaded");
        assertThat(report.removed()).containsExactly("removed");
        assertThat(report.loadFailed()).containsExactly("failed");
        assertThat(report.isEmpty()).isFalse();
        for (var snapshot : List.of(report.loaded(), report.removed(), report.loadFailed())) {
            assertThatThrownBy(snapshot::clear).isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
