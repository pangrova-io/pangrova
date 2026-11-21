package org.pangrova.indexing.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IndexFileReadResultTest {

    @Test
    void presentBytesCannotBeChangedThroughInputOrReturnedArray() {
        byte[] source = {1, 2, 3};
        IndexFileReadResult result = IndexFileReadResult.present(source);
        source[0] = 9;
        result.bytes()[1] = 9;

        assertThat(result.bytes()).containsExactly(1, 2, 3);
    }
}
