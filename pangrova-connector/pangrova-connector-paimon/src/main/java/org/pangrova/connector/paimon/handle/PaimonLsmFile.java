package org.pangrova.connector.paimon.handle;

import org.pangrova.spi.connector.metadata.table.ColumnMapping;
import org.pangrova.spi.connector.metadata.table.TypePromotion;

import java.util.List;

/** Exact physical file and sequence bounds from the Paimon manifest. */
public record PaimonLsmFile(PaimonPhysicalFile physical, long minSequenceNumber, long maxSequenceNumber,
        PaimonDeletionVector deletionVector, List<ColumnMapping> columnMappings, List<TypePromotion> typePromotions) {
    public PaimonLsmFile {
        java.util.Objects.requireNonNull(physical, "physical");
        columnMappings = List.copyOf(columnMappings);
        typePromotions = List.copyOf(typePromotions);
        if (minSequenceNumber < 0 || maxSequenceNumber < minSequenceNumber) {
            throw new IllegalArgumentException("Invalid Paimon manifest sequence bounds");
        }
    }
}
