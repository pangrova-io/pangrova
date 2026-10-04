# Actual registration export shares production registration and object code.
# It initializes no JNI, memory or filesystem runtime and performs no SQL parse.
# Generated artifacts are packaging outputs, not dependencies of these objects:
# this prevents exporter -> manifest -> exporter build cycles.
find_package(Python3 3.10 COMPONENTS Interpreter REQUIRED)
set(PANGROVA_FUNCTION_TOOLS
  "${CMAKE_CURRENT_SOURCE_DIR}/../pangrova-protocol/tools/function-contracts")
set(PANGROVA_BINDING_DECLARATION
  "${CMAKE_CURRENT_SOURCE_DIR}/../pangrova-protocol/src/main/resources/function-contracts/sql-bindings.json")
set(PANGROVA_STRUCTURAL_CONTRACT_HEADER
  "${CMAKE_CURRENT_BINARY_DIR}/generated-function-contracts/StructuralContractIdentities.h")
set(PANGROVA_PHYSICAL_TYPE_BRIDGE_HEADER
  "${CMAKE_CURRENT_BINARY_DIR}/generated-function-contracts/CanonicalPhysicalTypeProjection.h")
set(PANGROVA_EXECUTION_SELECTION_DECLARATION
  "${CMAKE_CURRENT_SOURCE_DIR}/../pangrova-protocol/src/main/resources/function-contracts/execution-selection.json")
set(PANGROVA_EXECUTION_SELECTION_HEADER
  "${CMAKE_CURRENT_BINARY_DIR}/generated-function-contracts/ExecutionSelection.h")
add_custom_command(OUTPUT "${PANGROVA_EXECUTION_SELECTION_HEADER}"
  COMMAND "${CMAKE_COMMAND}" -E env PYTHONDONTWRITEBYTECODE=1
    "${Python3_EXECUTABLE}" "${PANGROVA_FUNCTION_TOOLS}/generate_execution_selection.py"
    --declaration "${PANGROVA_EXECUTION_SELECTION_DECLARATION}"
    --cpp-output "${PANGROVA_EXECUTION_SELECTION_HEADER}"
  DEPENDS "${PANGROVA_EXECUTION_SELECTION_DECLARATION}"
    "${PANGROVA_FUNCTION_TOOLS}/generate_execution_selection.py"
    "${PANGROVA_FUNCTION_TOOLS}/contract_generation_inputs.py"
    "${PANGROVA_FUNCTION_TOOLS}/execution_selection.py"
    "${PANGROVA_FUNCTION_TOOLS}/generate_special_type_programs.py"
    "${PANGROVA_FUNCTION_TOOLS}/special_type_programs.py"
    "${PANGROVA_FUNCTION_TOOLS}/registration_catalog.py"
    "${PANGROVA_FUNCTION_TOOLS}/registered_signature.py"
    "${PANGROVA_FUNCTION_TOOLS}/scalar_implementations.py"
    "${PANGROVA_FUNCTION_TOOLS}/type_formula.py"
  VERBATIM)
add_custom_command(OUTPUT "${PANGROVA_STRUCTURAL_CONTRACT_HEADER}" "${PANGROVA_PHYSICAL_TYPE_BRIDGE_HEADER}"
  COMMAND "${CMAKE_COMMAND}" -E env PYTHONDONTWRITEBYTECODE=1
    "${Python3_EXECUTABLE}" "${PANGROVA_FUNCTION_TOOLS}/generate_structural_contracts.py"
    --declaration "${PANGROVA_BINDING_DECLARATION}"
    --output "${PANGROVA_STRUCTURAL_CONTRACT_HEADER}"
    --physical-output "${PANGROVA_PHYSICAL_TYPE_BRIDGE_HEADER}"
  DEPENDS "${PANGROVA_BINDING_DECLARATION}"
    "${PANGROVA_FUNCTION_TOOLS}/generate_structural_contracts.py"
    "${PANGROVA_FUNCTION_TOOLS}/binding_declarations.py"
    "${PANGROVA_FUNCTION_TOOLS}/binding_manifest.py"
    "${PANGROVA_FUNCTION_TOOLS}/registration_catalog.py"
    "${PANGROVA_FUNCTION_TOOLS}/registered_signature.py"
    "${PANGROVA_FUNCTION_TOOLS}/physical_type_bridge.py"
    "${PANGROVA_FUNCTION_TOOLS}/physical_type_manifest.py"
    "${PANGROVA_FUNCTION_TOOLS}/type_formula.py"
  VERBATIM)
add_custom_target(pangrova_structural_contracts
  DEPENDS "${PANGROVA_STRUCTURAL_CONTRACT_HEADER}" "${PANGROVA_EXECUTION_SELECTION_HEADER}"
    "${PANGROVA_PHYSICAL_TYPE_BRIDGE_HEADER}")
add_dependencies(pangrova_native_objects pangrova_structural_contracts)
target_include_directories(pangrova_native_objects PUBLIC
  "${CMAKE_CURRENT_BINARY_DIR}/generated-function-contracts")

add_executable(pangrova_function_export
  "${CMAKE_CURRENT_SOURCE_DIR}/tools/function-contracts/FunctionExportMain.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/tools/function-contracts/RegisteredFunctionExport.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/tools/function-contracts/CanonicalTypeDescriptorExport.cpp"
  "${CMAKE_CURRENT_SOURCE_DIR}/tools/function-contracts/FunctionResolverOracle.cpp")
target_include_directories(pangrova_function_export PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/src"
  "${CMAKE_CURRENT_SOURCE_DIR}/tools/function-contracts")
target_link_libraries(pangrova_function_export PRIVATE pangrova_native_objects)
if(VELOX_FOUND AND VELOX_MONO_LIB)
  if(APPLE)
    target_link_options(pangrova_function_export PRIVATE "-Wl,-force_load,${VELOX_MONO_LIB}")
    target_link_libraries(pangrova_function_export PRIVATE "-lc++abi")
  else()
    target_link_options(pangrova_function_export PRIVATE
      "-Wl,--whole-archive" "${VELOX_MONO_LIB}" "-Wl,--no-whole-archive")
  endif()
endif()
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(pangrova_function_export PRIVATE
    -Wall -Wextra -Werror -Wno-unused-parameter)
endif()
