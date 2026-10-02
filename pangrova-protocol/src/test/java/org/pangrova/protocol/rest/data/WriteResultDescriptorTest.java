package org.pangrova.protocol.rest.data;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WriteResultDescriptorTest {
    @Test
    void metadataOwnershipSurvivesInputAndAccessorMutation() {
        byte[] bytes = {1, 2, 3};
        var result = new WriteResultDescriptor(List.of(), 0, 0, bytes);
        int hash = result.hashCode();
        bytes[0] = 8;
        result.commitMetadata()[1] = 9;
        assertThat(result.commitMetadata()).containsExactly(1, 2, 3);
        assertThat(result.hashCode()).isEqualTo(hash);
        assertThat(result).isEqualTo(new WriteResultDescriptor(List.of(), 0, 0, new byte[] {1, 2, 3}));
    }
}
