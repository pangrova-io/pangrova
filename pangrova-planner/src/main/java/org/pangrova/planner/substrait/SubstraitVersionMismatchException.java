package org.pangrova.planner.substrait;

import org.pangrova.common.exception.PangrovaErrorCode;

/**
 * Thrown when a Substrait Plan received over the wire fails the
 * {@link SubstraitProtocolVersion#validate(io.substrait.proto.Plan)} check.
 *
 * <p>This is a producer/consumer-drift signal: the plan was built against
 * a different Substrait spec major version than this consumer supports, or
 * its required {@code Version} sub-message is missing/incomplete. The
 * exception is intentionally distinct from {@link SubstraitConversionException}
 * so callers can fail fast at the boundary instead of mistaking the issue
 * for a node-conversion bug deeper in execution.
 */
public final class SubstraitVersionMismatchException extends SubstraitConversionException {

    public SubstraitVersionMismatchException(String message) {
        super(PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE, message);
    }
}
