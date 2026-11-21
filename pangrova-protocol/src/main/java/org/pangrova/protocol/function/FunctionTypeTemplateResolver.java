package org.pangrova.protocol.function;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.substrait.proto.Type;
import org.pangrova.protocol.v2.FunctionTypeTemplate;

/** Resolves the selected signature only; failure cannot select a different implementation. */
final class FunctionTypeTemplateResolver {
    private final FunctionPhysicalTypeProjector projector;
    private int remaining = 4096;

    FunctionTypeTemplateResolver(FunctionPhysicalTypeProjector projector) {
        this.projector = projector;
    }

    Type resolve(FunctionTypeTemplate template, FunctionSignatureBindings bindings) {
        return resolve(template, bindings, 0);
    }

    private Type resolve(FunctionTypeTemplate template, FunctionSignatureBindings bindings, int depth) {
        if (--remaining < 0 || depth > 64 || !template.getUnknownFields().asMap().isEmpty()
                || template.getSlotsCount() > 256) {
            throw new IllegalArgumentException("Selected result template is unknown or exceeds its bounds");
        }
        if (template.hasTypeVariable()) return bindings.type(template.getTypeVariable());
        if (template.hasOpaqueRegisteredType()) FunctionRegisteredTypeAst.validate(template.getOpaqueRegisteredType());
        if (!template.hasSkeleton()) {
            throw new IllegalArgumentException("Selected result has no executable canonical type projection");
        }
        Message result = template.getSkeleton();
        var paths = new java.util.HashSet<java.util.List<org.pangrova.protocol.v2.FunctionTypePathStep>>();
        for (var slot : template.getSlotsList()) {
            if (!slot.getUnknownFields().asMap().isEmpty() || !paths.add(slot.getPathList())) {
                throw new IllegalArgumentException("Selected result has an unknown or duplicate substitution");
            }
            var leaf = FunctionTypePath.read(result, slot.getPathList());
            Object replacement;
            if (slot.hasType() && leaf.value() instanceof Type) {
                replacement = resolve(slot.getType(), bindings, depth + 1);
            } else if (slot.hasInteger() && leaf.field().getType() == FieldDescriptor.Type.INT32) {
                replacement = bindings.resolveInteger(slot.getInteger());
            } else {
                throw new IllegalArgumentException("Selected result substitution has no canonical projection");
            }
            result = FunctionTypePath.replace(result, slot.getPathList(), replacement);
        }
        return projector.project((Type) result).bindingType();
    }
}
