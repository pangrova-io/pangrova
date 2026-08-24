package org.pangrova.indexing.engine.bitmap;

import org.roaringbitmap.RoaringBitmap;

/**
 * Accumulates row positions into a RoaringBitmap instance.
 *
 * <p>Wraps the official {@link RoaringBitmap} to provide an incremental
 * accumulation API for the bitmap index build path.</p>
 */
final class ContainerAccumulator {

    private final RoaringBitmap bitmap = new RoaringBitmap();

    /**
     * Adds a row position to the accumulator.
     */
    void add(int position) {
        bitmap.add(position);
    }

    /**
     * Returns the accumulated bitmap, run-optimized for compact serialization.
     */
    RoaringBitmap toBitmap() {
        bitmap.runOptimize();
        return bitmap;
    }
}
