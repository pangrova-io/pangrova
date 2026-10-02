package org.pangrova.worker.ai.text;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class Bm25NativeScorerTest {

    @Test
    void javaToNativeAbiProducesExpectedScores() {
        Bm25ScoreRequest request = new Bm25ScoreRequest(
                new double[]{1.5},
                new int[]{2, 1},
                new int[]{100, 200},
                2, 1, 1.2, 0.75, 150.0);

        double[] scores = Bm25NativeScorer.scoreBatch(request);

        assertThat(scores).hasSize(2);
        assertThat(scores[0]).isCloseTo(expected(1.5, 2, 100), within());
        assertThat(scores[1]).isCloseTo(expected(1.5, 1, 200), within());
    }

    @Test
    void rejectsInconsistentArrayShapesBeforeJni() {
        assertThatThrownBy(() -> new Bm25ScoreRequest(
                new double[]{1.0}, new int[]{1}, new int[]{10, 20},
                2, 1, 1.2, 0.75, 15.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("termFreqs length");
    }

    private static double expected(double idf, double tf, double docLength) {
        double k1 = 1.2;
        double b = 0.75;
        double avg = 150.0;
        double norm = 1.0 - b + b * (docLength / avg);
        return idf * (tf * (k1 + 1.0)) / (tf + k1 * norm);
    }

    private static org.assertj.core.data.Offset<Double> within() {
        return org.assertj.core.data.Offset.offset(1e-9);
    }
}
