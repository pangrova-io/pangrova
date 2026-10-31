/**
 * Canonical physical write bindings, attempt staging and receipt integrity.
 * Does not execute writes or publish connector commits.
 *
 * <p>Owned by pangrova-protocol; consumed by engine, connector and control-plane adapters.
 * Handwritten Java APIs move with the repository; canonical wire compatibility is governed separately.</p>
 */
package org.pangrova.protocol.write;
