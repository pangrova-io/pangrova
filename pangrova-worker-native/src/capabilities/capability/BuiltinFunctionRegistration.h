#pragma once

namespace pangrova::capability {

// One registration source shared by runtime startup and artifact generation.
// It does not initialize JNI, memory, I/O, filesystem or execution operators.
void registerBuiltinFunctions();

}  // namespace pangrova::capability
