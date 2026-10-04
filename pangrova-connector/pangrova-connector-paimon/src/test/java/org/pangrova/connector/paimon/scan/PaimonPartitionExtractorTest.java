package org.pangrova.connector.paimon.scan;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.metadata.table.ColumnMapping;
import org.pangrova.spi.connector.metadata.table.TypePromotion;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Schema evolution results own their column mappings and type promotions. */
class PaimonPartitionExtractorTest {

    @Test
    void inputColumnMappingsCannotChangeTheConstructedResult() {
        var mapping = new ColumnMapping(1, 2, "old", "new");
        var mappings = new ArrayList<>(List.of(mapping));
        var result = new PaimonPartitionExtractor.SchemaEvolutionResult(mappings, List.of());

        mappings.clear();

        assertThat(result.columnMappings()).containsExactly(mapping);
    }

    @Test
    void inputTypePromotionsCannotChangeTheConstructedResult() {
        var promotion = new TypePromotion(2, "INTEGER", "BIGINT", "new");
        var promotions = new ArrayList<>(List.of(promotion));
        var result = new PaimonPartitionExtractor.SchemaEvolutionResult(List.of(), promotions);

        promotions.clear();

        assertThat(result.typePromotions()).containsExactly(promotion);
    }

    @Test
    void columnMappingsAccessorDoesNotPermitMutation() {
        var mapping = new ColumnMapping(1, 2, "old", "new");
        var result = new PaimonPartitionExtractor.SchemaEvolutionResult(
                new ArrayList<>(List.of(mapping)), List.of());

        assertThatThrownBy(() -> result.columnMappings().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(result.columnMappings()).containsExactly(mapping);
    }

    @Test
    void typePromotionsAccessorDoesNotPermitMutation() {
        var promotion = new TypePromotion(2, "INTEGER", "BIGINT", "new");
        var result = new PaimonPartitionExtractor.SchemaEvolutionResult(
                List.of(), new ArrayList<>(List.of(promotion)));

        assertThatThrownBy(() -> result.typePromotions().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(result.typePromotions()).containsExactly(promotion);
    }
}
