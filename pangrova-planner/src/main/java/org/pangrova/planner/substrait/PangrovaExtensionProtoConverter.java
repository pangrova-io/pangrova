package org.pangrova.planner.substrait;

import com.google.protobuf.Any;
import io.substrait.extension.AdvancedExtension;
import io.substrait.extension.ExtensionProtoConverter;

/** Serializes Pangrova-owned advanced extensions without private Substrait fields. */
final class PangrovaExtensionProtoConverter extends ExtensionProtoConverter<
        PangrovaOptimization, PangrovaExtensionProtoConverter.Enhancement> {

    interface Enhancement extends AdvancedExtension.Enhancement {
        Any toProto();
    }

    @Override
    protected Any toProto(PangrovaOptimization optimization) {
        return optimization.toProto();
    }

    @Override
    protected Any toProto(Enhancement enhancement) {
        return enhancement.toProto();
    }
}
