/**
 * SPI for distributed (shared) data caching across workers.
 *
 * <p>Pangrova workers cache data blocks locally using the native async data
 * cache (RAM) and optional SSD cache. This package defines
 * the SPI for an additional <em>distributed</em> cache tier that
 * allows workers to share cached data via an external backend
 * (Redis, shared filesystem, object store tiering).</p>
 *
 * <h3>Key types</h3>
 * <ul>
 *   <li>{@link org.pangrova.spi.cache.CacheTopology} --
 *       enum describing the cache organization model</li>
 *   <li>{@link org.pangrova.spi.cache.DistributedCacheStore} --
 *       main SPI interface for get/put/invalidate operations</li>
 *   <li>{@link org.pangrova.spi.cache.DistributedCacheStoreFactory} --
 *       factory for creating backend-specific store instances</li>
 *   <li>{@link org.pangrova.spi.cache.CacheKey} --
 *       immutable cache key (file path + byte range)</li>
 *   <li>{@link org.pangrova.spi.cache.CacheEntry} --
 *       cached data block with metadata</li>
 *   <li>{@link org.pangrova.spi.cache.CacheStoreStats} --
 *       monitoring statistics</li>
 * </ul>
 *
 * <p>G-CACHE-08: distributed/detached cache layer SPI.</p>
 */
package org.pangrova.spi.cache;
