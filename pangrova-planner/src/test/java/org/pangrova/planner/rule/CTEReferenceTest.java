package org.pangrova.planner.rule;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CTEReferenceTest {

    @Test
    void validRecord() {
        var ref = new CTEReference("cte1", 5000, 3);
        assertThat(ref.cteName()).isEqualTo("cte1");
        assertThat(ref.cteCost()).isEqualTo(5000);
        assertThat(ref.referenceCount()).isEqualTo(3);
    }

    @Test
    void blankName_throws() {
        assertThatThrownBy(() -> new CTEReference("", 100, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeCost_throws() {
        assertThatThrownBy(() -> new CTEReference("cte1", -1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeRefCount_throws() {
        assertThatThrownBy(() -> new CTEReference("cte1", 100, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
