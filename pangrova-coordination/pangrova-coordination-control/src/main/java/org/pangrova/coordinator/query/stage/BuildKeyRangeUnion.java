package org.pangrova.coordinator.query.stage;

import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;

import java.util.ArrayList;
import java.util.List;

/** Conservative bounded coverage of one build key, owned by the stage's report lock. */
final class BuildKeyRangeUnion {
    private final List<BuildKeyRange> ranges = new ArrayList<>(BuildKeyRange.MAX_RANGES);
    private boolean collapsed;

    void addAll(List<BuildKeyRange> additions) {
        for (var addition : additions) add(addition);
    }

    List<BuildKeyRange> snapshot() {
        return List.copyOf(ranges);
    }

    private void add(BuildKeyRange addition) {
        if (collapsed) {
            ranges.set(0, ranges.get(0).merge(addition));
            return;
        }

        int first = 0;
        while (first < ranges.size() && separated(ranges.get(first), addition)) first++;
        int end = first;
        while (end < ranges.size() && !separated(addition, ranges.get(end))) {
            addition = addition.merge(ranges.get(end++));
        }
        if (first != end) {
            ranges.set(first, addition);
            ranges.subList(first + 1, end).clear();
        } else if (ranges.size() < BuildKeyRange.MAX_RANGES) {
            ranges.add(first, addition);
        } else {
            var coverage = ranges.get(0).merge(ranges.get(ranges.size() - 1)).merge(addition);
            ranges.clear();
            ranges.add(coverage);
            collapsed = true;
        }
    }

    private static boolean separated(BuildKeyRange left, BuildKeyRange right) {
        return left.maximum() < right.minimum() && left.maximum() + 1 < right.minimum();
    }
}
