package org.pangrova.connector.spi.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteAuthorizationReceiptTest {
    private static final WriteDeclarationSnapshot DECLARATION = new WriteDeclarationSnapshot(
            new QualifiedTableName("lake", "sales", "documents"), "a".repeat(64), "b".repeat(64), false);

    @Test
    void replayIdentityUsesContentEqualityAndDefensiveCopies() {
        byte[] bytes = {1, 2, 3};
        var original = new WriteAuthorizationReceipt(bytes, DECLARATION);
        var replayed = new WriteAuthorizationReceipt(new byte[]{1, 2, 3}, DECLARATION);
        bytes[0] = 9;
        original.authorizationScope()[1] = 9;
        assertThat(original.authorizationScope()).containsExactly(1, 2, 3);
        assertThat(original).isEqualTo(replayed);
        assertThat(original.hashCode()).isEqualTo(replayed.hashCode());
        var receipts = new HashSet<WriteAuthorizationReceipt>();
        receipts.add(original);
        receipts.add(replayed);
        assertThat(receipts).hasSize(1);
        assertThat(original).isNotEqualTo(new WriteAuthorizationReceipt(new byte[]{1, 2, 4}, DECLARATION));
    }

    @Test
    void ordinaryWriteStillRequiresExplicitAuthorityAndAbsenceDeclaration() {
        assertThatThrownBy(() -> new WriteAuthorizationReceipt(new byte[0], DECLARATION))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WriteAuthorizationReceipt(new byte[]{1}, null))
                .isInstanceOf(NullPointerException.class);
        assertThat(WriteDeclarationSnapshot.generatedColumnsDigest(new byte[]{1}))
                .isNotEqualTo(WriteDeclarationSnapshot.generatedColumnsDigest(new byte[]{2}));
    }
}
