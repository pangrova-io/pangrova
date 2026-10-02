package org.pangrova.protocol;

import java.util.List;

import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.spi.annotation.Internal;

/** Canonical Substrait fragment and the execution bindings required by its operators. */
@Internal
public record BoundExecutionPlan(
        byte[] planBytes,
        List<OperatorBinding> operatorBindings,
        List<ResultColumnLogicalType> resultColumnTypes) {

    public BoundExecutionPlan {
        planBytes = planBytes.clone();
        operatorBindings = List.copyOf(operatorBindings);
        resultColumnTypes = List.copyOf(resultColumnTypes);
    }

    public BoundExecutionPlan(byte[] planBytes, List<OperatorBinding> operatorBindings) {
        this(planBytes, operatorBindings, List.of());
    }

    @Override
    public byte[] planBytes() {
        return planBytes.clone();
    }
}
