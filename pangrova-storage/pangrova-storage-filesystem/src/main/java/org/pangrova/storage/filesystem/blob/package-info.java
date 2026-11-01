/**
 * Filesystem blob provider; deployment roots are administrator-owned persistent storage.
 *
 * <p>Owned by pangrova-storage-filesystem. Consumers are storage providers and explicit
 * process assembly; governed FILE and COPY consumers use the API. Authorization, source
 * version authority and query lifecycle do not belong in this package.
 * Provider constructors are internal assembly APIs, not stable domain extension contracts.
 */
package org.pangrova.storage.filesystem.blob;
