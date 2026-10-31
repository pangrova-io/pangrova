#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>

namespace pangrova::capability {

inline int64_t typeFormulaAdd(int64_t left, int64_t right) {
  int64_t result;
  if (__builtin_add_overflow(left, right, &result)) throw std::overflow_error("Type formula addition overflow");
  return result;
}

inline int64_t typeFormulaSubtract(int64_t left, int64_t right) {
  int64_t result;
  if (__builtin_sub_overflow(left, right, &result)) throw std::overflow_error("Type formula subtraction overflow");
  return result;
}

inline int64_t typeFormulaMultiply(int64_t left, int64_t right) {
  int64_t result;
  if (__builtin_mul_overflow(left, right, &result)) throw std::overflow_error("Type formula multiplication overflow");
  return result;
}

inline void typeFormulaRequireDivision(int64_t left, int64_t right) {
  if (right == 0 || (left == std::numeric_limits<int64_t>::min() && right == -1)) {
    throw std::overflow_error("Type formula division/remainder is undefined or overflows");
  }
}

inline int64_t typeFormulaDivide(int64_t left, int64_t right) {
  typeFormulaRequireDivision(left, right);
  return left / right;
}

inline int64_t typeFormulaRemainder(int64_t left, int64_t right) {
  typeFormulaRequireDivision(left, right);
  return left % right;
}

inline int64_t typeFormulaNegate(int64_t value) { return typeFormulaSubtract(0, value); }
inline int64_t typeFormulaMin(int64_t left, int64_t right) { return std::min(left, right); }
inline int64_t typeFormulaMax(int64_t left, int64_t right) { return std::max(left, right); }
inline int64_t typeFormulaConditional(int64_t condition, int64_t whenTrue, int64_t whenFalse) {
  return condition == 0 ? whenFalse : whenTrue;
}

}  // namespace pangrova::capability
