#include "capabilities/capability/FunctionTypeConstraintCompiler.h"

#include <charconv>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace pangrova::capability {
namespace {
using Formula = protocol::v2::FunctionTypeFormula;
using Operation = protocol::v2::FunctionFormulaOperation;
using namespace protocol::v2;

bool digit(char value) { return value >= '0' && value <= '9'; }
bool letter(char value) { return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z'); }

struct Operator { int precedence; Operation operation; };
const std::map<std::string_view, Operator> operators = {
    {"==", {2, FUNCTION_FORMULA_OPERATION_EQUAL}}, {"!=", {2, FUNCTION_FORMULA_OPERATION_NOT_EQUAL}},
    {"<", {3, FUNCTION_FORMULA_OPERATION_LESS}}, {"<=", {3, FUNCTION_FORMULA_OPERATION_LESS_OR_EQUAL}},
    {">", {3, FUNCTION_FORMULA_OPERATION_GREATER}}, {">=", {3, FUNCTION_FORMULA_OPERATION_GREATER_OR_EQUAL}},
    {"+", {4, FUNCTION_FORMULA_OPERATION_ADD}}, {"-", {4, FUNCTION_FORMULA_OPERATION_SUBTRACT}},
    {"*", {5, FUNCTION_FORMULA_OPERATION_MULTIPLY}}, {"/", {5, FUNCTION_FORMULA_OPERATION_DIVIDE}},
    {"%", {5, FUNCTION_FORMULA_OPERATION_REMAINDER}}};

std::vector<std::string_view> tokenize(std::string_view source) {
  if (source.empty() || source.size() > 65536) throw std::invalid_argument("Registered formula text exceeds its bound");
  std::vector<std::string_view> result;
  for (size_t cursor = 0; cursor < source.size();) {
    const auto value = source[cursor];
    if (value == ' ' || value == '\t' || value == '\n' || value == '\r') { ++cursor; continue; }
    const auto start = cursor++;
    if (digit(value)) {
      while (cursor < source.size() && digit(source[cursor])) ++cursor;
    } else if (letter(value)) {
      while (cursor < source.size() && (letter(source[cursor]) || digit(source[cursor]) || source[cursor] == '_')) ++cursor;
    } else if (cursor < source.size() && source[cursor] == '=' && (value == '=' || value == '!' || value == '<' || value == '>')) {
      ++cursor;
    } else if (std::string_view("+-*/%?:(),<>").find(value) == std::string_view::npos) {
      throw std::invalid_argument("Registered formula contains an unknown token");
    }
    result.push_back(source.substr(start, cursor - start));
    if (result.size() > 8192) throw std::invalid_argument("Registered formula token count exceeds its bound");
  }
  if (result.empty()) throw std::invalid_argument("Registered formula is empty");
  return result;
}

class Parser final {
 public:
  Parser(std::string_view source, const std::unordered_set<std::string>& variables)
      : tokens_(tokenize(source)), variables_(variables) {}

  Formula parse() {
    auto result = expression(0, 0);
    if (cursor_ != tokens_.size()) throw std::invalid_argument("Registered formula contains trailing tokens");
    return result;
  }

 private:
  std::string_view peek() const { return cursor_ < tokens_.size() ? tokens_[cursor_] : std::string_view{}; }
  std::string_view take() {
    if (cursor_ >= tokens_.size()) throw std::invalid_argument("Registered formula is missing an operand");
    return tokens_[cursor_++];
  }
  void require(std::string_view expected) {
    if (take() != expected) throw std::invalid_argument("Registered formula delimiter mismatch");
  }
  Formula node(Operation operation) {
    if (++nodes_ > 2048) throw std::invalid_argument("Registered formula node count exceeds its bound");
    Formula result;
    result.set_operation(operation);
    return result;
  }
  Formula binary(Operation operation, Formula left, Formula right) {
    auto result = node(operation);
    *result.add_operands() = std::move(left);
    *result.add_operands() = std::move(right);
    return result;
  }
  Formula expression(int minimum, int depth) {
    if (depth > 64) throw std::invalid_argument("Registered formula depth exceeds its bound");
    auto left = prefix(depth);
    for (;;) {
      const auto found = operators.find(peek());
      if (found == operators.end() || found->second.precedence < minimum) break;
      const auto operation = found->second;
      take();
      left = binary(operation.operation, std::move(left), expression(operation.precedence + 1, depth + 1));
    }
    if (minimum <= 1 && peek() == "?") {
      take();
      auto whenTrue = expression(0, depth + 1);
      require(":");
      auto result = node(FUNCTION_FORMULA_OPERATION_CONDITIONAL);
      *result.add_operands() = std::move(left);
      *result.add_operands() = std::move(whenTrue);
      *result.add_operands() = expression(1, depth + 1);
      left = std::move(result);
    }
    return left;
  }
  Formula prefix(int depth) {
    const auto token = take();
    if (token == "-") {
      auto result = node(FUNCTION_FORMULA_OPERATION_NEGATE);
      *result.add_operands() = expression(6, depth + 1);
      return result;
    }
    if (token == "(") {
      auto result = expression(0, depth + 1);
      require(")");
      return result;
    }
    if (token == "min" || token == "max") {
      require("(");
      auto left = expression(0, depth + 1);
      require(",");
      auto right = expression(0, depth + 1);
      require(")");
      return binary(token == "min" ? FUNCTION_FORMULA_OPERATION_MIN : FUNCTION_FORMULA_OPERATION_MAX,
                    std::move(left), std::move(right));
    }
    if (digit(token.front())) {
      auto result = node(FUNCTION_FORMULA_OPERATION_INTEGER);
      int64_t value = 0;
      const auto [end, error] = std::from_chars(token.data(), token.data() + token.size(), value);
      if (error != std::errc{} || end != token.data() + token.size()) {
        throw std::invalid_argument("Registered formula literal exceeds signed 64-bit");
      }
      result.set_integer_value(value);
      return result;
    }
    if (!variables_.contains(std::string(token))) throw std::invalid_argument("Registered formula references an unknown variable");
    auto result = node(FUNCTION_FORMULA_OPERATION_VARIABLE);
    result.set_variable_name(std::string(token));
    return result;
  }

  const std::vector<std::string_view> tokens_;
  const std::unordered_set<std::string>& variables_;
  size_t cursor_{0};
  size_t nodes_{0};
};
}  // namespace

protocol::v2::FunctionTypeFormula compileRegisteredTypeConstraint(
    std::string_view source, const std::unordered_set<std::string>& integerVariables) {
  return Parser(source, integerVariables).parse();
}
}  // namespace pangrova::capability
