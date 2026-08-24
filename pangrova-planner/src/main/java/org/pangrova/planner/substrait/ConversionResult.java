package org.pangrova.planner.substrait;

import io.substrait.plan.Plan;

import java.util.List;

/**
 * Immutable result of a single {@link SubstraitConverter} conversion.
 *
 * <p>Carries the converted Substrait {@link Plan} together with the non-equi
 * (cross) join details accumulated during that specific conversion. Returning
 * these together eliminates the previous shared-mutable-state hand-off
 * ({@code SubstraitConverter.lastCrossJoinDetails}) that caused cross-join detail
 * cross-contamination between concurrent queries sharing one converter instance.</p>
 *
 * @param plan the converted Substrait plan
 * @param crossJoinDetails non-equi join details for this conversion; consumed by
 *        the serializer to inject expression/type fields into CrossRel proto
 */
public record ConversionResult(Plan plan, List<CrossJoinDetail> crossJoinDetails) {
}
