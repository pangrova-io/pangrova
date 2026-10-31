/**
 * Internal Worker connector resource binding contracts shared by catalog providers and
 * write adapters. Implementations own exact catalog-revision leases and decoded handles;
 * consumers must use those bindings rather than select a catalog or start a new write.
 * JNI entry points and catalog runtime implementations do not belong in this package.
 */
package org.pangrova.worker.nativebridge.connector.spi;
