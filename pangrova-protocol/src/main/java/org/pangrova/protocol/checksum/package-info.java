/**
 * Canonical protocol checksum construction over caller-selected bytes.
 * Does not choose domain serialization, attest authorization, or own task validation.
 *
 * <p>Owned by pangrova-protocol; consumed by engine, connector and control-plane adapters.
 * Handwritten Java APIs move with the repository; canonical wire compatibility is governed separately.</p>
 */
package org.pangrova.protocol.checksum;
