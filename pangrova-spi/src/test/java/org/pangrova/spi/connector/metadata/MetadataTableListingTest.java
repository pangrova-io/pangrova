package org.pangrova.spi.connector.metadata;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataTableListingTest {
    @Test
    void freezesFiltersWithoutTreatingNamesOrValuesAsSql() {
        var input = new HashMap<String, String>();
        input.put("type", "TAG' OR TRUE --");
        input.put("odd.column", "value");
        var listing = new MetadataTableListing(MetadataTableKind.REFS, input);
        input.clear();
        assertThat(listing.equalityFilters()).containsEntry("type", "TAG' OR TRUE --");
        assertThat(listing.equalityFilters().keySet()).containsExactly("odd.column", "type");
        assertThatThrownBy(() -> listing.equalityFilters().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsIncompleteFiltersInsteadOfDroppingThem() {
        assertThatThrownBy(() -> new MetadataTableListing(MetadataTableKind.REFS, Map.of(" ", "TAG")))
                .isInstanceOf(IllegalArgumentException.class);
        var invalid = new HashMap<String, String>();
        invalid.put("type", null);
        assertThatThrownBy(() -> new MetadataTableListing(MetadataTableKind.REFS, invalid))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void directMetadataSourceHasNoImplicitFilter() {
        assertThat(new MetadataTableListing(MetadataTableKind.TAGS).equalityFilters()).isEmpty();
    }
}
