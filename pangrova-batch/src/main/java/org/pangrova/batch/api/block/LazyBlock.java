package org.pangrova.batch.api.block;

import org.pangrova.batch.api.enums.BlockTypeId;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Lazily-loaded {@link Block} that defers materialization until first access.
 * The supplied loader is invoked at most once; subsequent accesses reuse
 * the cached result.
 *
 * <p>Thread-safe: uses volatile + double-checked locking for lazy init.</p>
 */
@Experimental
public final class LazyBlock implements Block {

    private final Supplier<Block> loader;
    private final int positionCount;
    private volatile Block loaded;

    /** Creates a block that invokes the loader on first access. */
    public LazyBlock(Supplier<Block> loader, int positionCount) {
        Objects.requireNonNull(loader, "loader must not be null");
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount must be non-negative");
        }
        this.loader = loader;
        this.positionCount = positionCount;
    }

    @Override
    public BlockTypeId typeId() {
        return getLoaded().typeId();
    }

    @Override
    public int getPositionCount() {
        return positionCount;
    }

    @Override
    public boolean isNull(int position) {
        return getLoaded().isNull(position);
    }

    @Override
    public int getInt(int position) {
        return getLoaded().getInt(position);
    }

    @Override
    public long getLong(int position) {
        return getLoaded().getLong(position);
    }

    @Override
    public double getDouble(int position) {
        return getLoaded().getDouble(position);
    }

    @Override
    public byte[] getSlice(int position) {
        return getLoaded().getSlice(position);
    }

    @Override
    public Block getRegion(int positionOffset, int length) {
        return getLoaded().getRegion(positionOffset, length);
    }

    private Block getLoaded() {
        Block result = loaded;
        if (result == null) {
            synchronized (this) {
                result = loaded;
                if (result == null) {
                    result = loader.get();
                    Objects.requireNonNull(result, "loader returned null Block");
                    loaded = result;
                }
            }
        }
        return result;
    }
}
