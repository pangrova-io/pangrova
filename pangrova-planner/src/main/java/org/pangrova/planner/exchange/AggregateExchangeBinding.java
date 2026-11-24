package org.pangrova.planner.exchange;

import com.google.protobuf.ByteString;
import io.substrait.proto.AggregationPhase;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.pangrova.protocol.function.catalog.FunctionCatalogView;
import org.pangrova.protocol.v2.FunctionKind;
import org.pangrova.protocol.v2.FunctionReferenceContract;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Exact SQL function selection for a state shared by different Workers. */
public record AggregateExchangeBinding(FunctionReferenceContract function, ByteString actualRegistrationSha256,
                                       String implementationBuildId, NamedStruct originalArguments,
                                       Type state, Type result) {
    static Optional<AggregateExchangeBinding> select(List<FunctionCatalogView> catalogues,
            String urn, String name, NamedStruct originalArguments) {
        AggregateExchangeBinding selected = null;
        for (FunctionCatalogView catalogue : catalogues) {
            var candidate = catalogue.findDefinition(FunctionKind.FUNCTION_KIND_AGGREGATE, urn, name);
            if (candidate.isEmpty()) {
                return Optional.empty();
            }
            var definition = candidate.orElseThrow();
            var reference = definition.getRef();
            if (!catalogue.isAvailable(reference)
                    || !definition.getAggregatePhasesList().contains(
                            AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE)
                    || !definition.getAggregatePhasesList().contains(
                            AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT)) {
                return Optional.empty();
            }
            var raw = catalogue.bindRawAggregate(reference, originalArguments, Set.of(), false);
            if (raw.signature().getConstantArgumentsList().contains(true)) {
                return Optional.empty();
            }
            var binding = new AggregateExchangeBinding(reference, raw.registration().getContractSha256(),
                    catalogue.manifest().getImplementationBuildId(),
                    originalArguments, raw.binding().intermediate().orElseThrow(), raw.binding().output());
            if (selected != null && !selected.equals(binding)) {
                return Optional.empty();
            }
            selected = binding;
        }
        return Optional.ofNullable(selected);
    }
}
