package org.pangrova.spi.cache;

import org.pangrova.spi.annotation.Experimental;

/**
 * Describes how data caching is organized across workers in the cluster.
 *
 * <p>The topology determines whether workers cache independently, use
 * cache-aware scheduling to maximize hit rates, or share a common
 * external cache store.</p>
 *
 * <h3>Topology comparison</h3>
 * <table>
 *   <tr><th>Topology</th><th>Cache shared?</th><th>Worker replacement cost</th><th>External dependency</th></tr>
 *   <tr><td>{@link #LOCAL_ONLY}</td><td>No</td><td>Full re-warm</td><td>None</td></tr>
 *   <tr><td>{@link #SHARED_NOTHING}</td><td>No</td><td>Partial (affinity routing)</td><td>None</td></tr>
 *   <tr><td>{@link #SHARED_STORE}</td><td>Yes</td><td>Minimal</td><td>Redis / shared FS / object store</td></tr>
 * </table>
 *
 * <p>G-CACHE-08: distributed/detached cache layer SPI.</p>
 */
@Experimental(reason = "G-CACHE-08: distributed cache topology model")
public enum CacheTopology {

    /**
     * Each worker caches data locally and independently. No cross-worker
     * sharing or coordination. This is the current (pre-G-CACHE-08) default.
     *
     * <p>When a worker is replaced, its entire cache is lost.</p>
     */
    LOCAL_ONLY,

    /**
     * Workers still cache locally, but the scheduler uses cache-locality
     * hints (via {@link org.pangrova.spi.coordination.CacheCatalogSummary})
     * to route splits to workers that already have relevant data cached.
     *
     * <p>No external store is needed. Cache loss on worker replacement is
     * mitigated by consistent-hashing or affinity-based scheduling, so
     * the replacement worker eventually re-warms the same key space.</p>
     */
    SHARED_NOTHING,

    /**
     * Workers share a common external cache store (e.g. Redis, a shared
     * filesystem, or object-store tiering). A cache entry written by one
     * worker is readable by all other workers.
     *
     * <p>When a worker is replaced, cached data survives in the external
     * store. The new worker can read it immediately without re-warming
     * from the source storage layer.</p>
     *
     * <p>Requires a {@link DistributedCacheStore} implementation to be
     * configured and wired.</p>
     */
    SHARED_STORE
}
