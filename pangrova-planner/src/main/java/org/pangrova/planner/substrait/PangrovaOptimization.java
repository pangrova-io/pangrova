package org.pangrova.planner.substrait;

import com.google.protobuf.Any;
import io.substrait.extension.AdvancedExtension;

/** Typed marker for Pangrova-owned, ignorable Substrait optimization hints. */
interface PangrovaOptimization extends AdvancedExtension.Optimization {

    Any toProto();
}
