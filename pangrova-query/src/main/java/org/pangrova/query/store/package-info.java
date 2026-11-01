/**
 * Process-local Query stores awaiting relocation to the dedicated Persistence memory backend.
 * Implementations own their admitted state and lifecycle, never query scheduling or JDBC.
 */
package org.pangrova.query.store;
