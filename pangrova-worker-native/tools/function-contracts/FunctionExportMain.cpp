#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>

#include "CanonicalTypeDescriptorExport.h"
#include "FunctionResolverOracle.h"
#include "RegisteredFunctionExport.h"
#include "capability/BuiltinFunctionRegistration.h"

int main(int argc, char** argv) {
  try {
    if (argc != 2) {
      throw std::runtime_error("Expected --registrations, --type-descriptor or --resolve-scalars");
    }
    const std::string command(argv[1]);
    if (command == "--type-descriptor") {
      std::cout << pangrova::capability::exportCanonicalTypeDescriptor() << '\n';
      return 0;
    }
    pangrova::capability::registerBuiltinFunctions();
    if (command == "--registrations") {
      std::cout << pangrova::capability::exportRegisteredFunctions() << '\n';
      return 0;
    }
    if (command != "--resolve-scalars") {
      throw std::runtime_error("Unknown function export command");
    }
    std::string request;
    while (std::getline(std::cin, request)) {
      std::cout << pangrova::capability::resolveRegisteredScalarOracle(request) << '\n';
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
