package org.pangrova.metadata.service;

import org.junit.jupiter.api.Test;
import org.pangrova.metadata.model.QualifiedTableRef;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataTagOperationsTest {

    private final MetadataTagOperations tags = new MetadataTagOperations();

    @Test
    void keepsTagsIsolatedByQualifiedTableAndColumn() {
        QualifiedTableRef orders = new QualifiedTableRef("catalog", "sales", "orders");
        QualifiedTableRef customers = new QualifiedTableRef("catalog", "sales", "customers");

        tags.setTag(orders, "id", "classification", "sensitive");

        assertThat(tags.listTags(orders, "id"))
                .containsEntry("classification", "sensitive");
        assertThat(tags.listTags(orders, "other")).isEmpty();
        assertThat(tags.listTags(customers, "id")).isEmpty();

        tags.unsetTag(orders, "id", "classification");
        assertThat(tags.listTags(orders, "id")).isEmpty();
    }

    @Test
    void rejectsMissingTagIdentityAndValue() {
        QualifiedTableRef table = new QualifiedTableRef("catalog", "sales", "orders");

        assertThatThrownBy(() -> tags.setTag(null, "id", "classification", "sensitive"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> tags.setTag(table, null, "classification", "sensitive"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> tags.setTag(table, "id", null, "sensitive"))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> tags.setTag(table, "id", "classification", null))
                .isInstanceOf(NullPointerException.class);
    }
}
