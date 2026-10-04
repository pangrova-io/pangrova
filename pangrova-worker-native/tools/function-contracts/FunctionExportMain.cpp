#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

#include "CanonicalTypeDescriptorExport.h"
#include "FunctionResolverOracle.h"
#include "RegisteredFunctionExport.h"
#include "capability/BuiltinFunctionRegistration.h"
#include "capability/FunctionCatalogAssembler.h"
#include "capability/FunctionContractEncoding.h"
#include "capability/NativeCapabilityInventory.h"
#include "velox/expression/FunctionRegistrySnapshot.h"

int main(int argc, char** argv) {
  try {
    if (argc != 2) {
      throw std::runtime_error("Expected --registrations, --catalogue, --type-descriptor, --type-projections or --resolve-scalars");
    }
    const std::string command(argv[1]);
    if (command == "--type-descriptor") {
      std::cout << pangrova::capability::exportCanonicalTypeDescriptor() << '\n';
      return 0;
    }
    if (command == "--type-projections") {
      std::cout << pangrova::capability::exportCanonicalPhysicalTypeProjections() << '\n';
      return 0;
    }
    pangrova::capability::registerBuiltinFunctions();
    const auto snapshot = facebook::velox::exec::FunctionRegistrySnapshot::capture();
    if (command == "--catalogue") {
      const auto publication = pangrova::capability::FunctionCatalogAssembler::assemble(
          snapshot, pangrova::capability::runtimeCapabilityInventory(*snapshot));
      const auto bytes = pangrova::capability::encodeFunctionContract(publication->inventory());
      std::cout.write(bytes.data(), bytes.size());
      if (!std::cout) throw std::runtime_error("Actual catalogue output write failed");
      std::cerr << "inventory_bytes=" << bytes.size()
                << " encoded_inventory_bytes=" << 4 * ((bytes.size() + 2) / 3) << '\n';
      return 0;
    }
    if (command == "--registrations") {
      std::cout << pangrova::capability::exportRegisteredFunctions(snapshot) << '\n';
      return 0;
    }
    if (command != "--resolve-scalars") {
      throw std::runtime_error("Unknown function export command");
    }
    std::string request;
    while (std::getline(std::cin, request)) {
      std::cout << pangrova::capability::resolveRegisteredScalarOracle(snapshot, request) << '\n';
    }
    if (!std::cin.eof()) {
      throw std::runtime_error("Function oracle input read failed");
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "Function export failed: " << error.what() << '\n';
    return 1;
  }
}
