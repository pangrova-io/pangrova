/**
 * Canonical logical type projection and validation owned by pangrova-protocol.
 *
 * <p>Coordinator, Worker, planner, connector and transport consumers use the shared
 * logical contract without introducing another type protocol. DeclaredLogicalTypes
 * currently projects the supported physical-write declaration domain; its neutral
 * name does not advertise complete connector read-type support. These helpers own
 * no runtime state, storage SDK objects or data-plane I/O.</p>
 */
package org.pangrova.protocol.type;
