// Copyright 2024 Pangrova Authors
//
// Provide RTTI typeinfo for __int128 / unsigned __int128.
//
// Velox (compiled with LLVM Clang 17) references these typeinfo symbols
// via dynamic_cast / typeid on int128 vectors.  AppleClang 14 does not
// emit them in its compiler-rt, so we supply them here as weak symbols
// to avoid "Undefined symbols" at link time.
//
// P2-FILE-2: This file is only needed on Apple platforms where the
// compiler-rt does not provide __int128 typeinfo. On Linux/glibc,
// these symbols are provided by libstdc++ or libc++abi.
//
// The mangled names follow the Itanium ABI:
//   _ZTIn  = typeinfo for __int128          (builtin type 'n')
//   _ZTIo  = typeinfo for unsigned __int128 (builtin type 'o')
//
// We use raw asm-level symbol definitions to avoid needing the internal
// __cxxabiv1::__fundamental_type_info class definition.

#if defined(__APPLE__)

#include <typeinfo>

// P2-COMPAT-1: Verify __int128 is available on this platform.
static_assert(sizeof(__int128) == 16, "__int128 must be 16 bytes");
static_assert(sizeof(unsigned __int128) == 16, "unsigned __int128 must be 16 bytes");

// On Apple platforms, the typeinfo for fundamental types consists of:
//   - vtable pointer for __cxxabiv1::__fundamental_type_info
//   - const char* name
//
// We reference the existing vtable from libc++abi, then lay out our
// typeinfo structures manually.

// Import the vtable for __fundamental_type_info from libc++abi
extern "C" const void *_ZTVN10__cxxabiv123__fundamental_type_infoE[];

// The vtable pointer in a typeinfo object points 2 entries past the start
// (past the offset-to-top and RTTI entries).
static const void *const _fti_vtable =
    &_ZTVN10__cxxabiv123__fundamental_type_infoE[2];

// Name strings for the types
static const char _name_n[] = "n";  // __int128
static const char _name_o[] = "o";  // unsigned __int128

// typeinfo for __int128
extern "C" __attribute__((weak, visibility("default")))
const void *const _ZTIn[] = {_fti_vtable, (const void *)_name_n};

// typeinfo for unsigned __int128
extern "C" __attribute__((weak, visibility("default")))
const void *const _ZTIo[] = {_fti_vtable, (const void *)_name_o};

#endif  // defined(__APPLE__)

