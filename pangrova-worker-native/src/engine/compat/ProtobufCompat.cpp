// Protobuf v5 compatibility shim.
// Velox's libvelox.a references google::protobuf::Message::CopyWithSourceCheck
// which exists in protobuf v5 but not in our hermetic v4.25.3. This file
// provides the exact symbol via asm label to satisfy the linker without
// requiring the v5 header declaration.
//
// Symbol (macOS): __ZN6google8protobuf7Message19CopyWithSourceCheckERS1_RKS1_
// Demangled: google::protobuf::Message::CopyWithSourceCheck(Message&, Message const&)
// This is a STATIC member function (no implicit this pointer).
//
// Semantics: CopyWithSourceCheck in v5 is CopyFrom + debug assertion that
// &to != &from. We delegate to CopyFrom which already handles this case.
#include <google/protobuf/message.h>

// Emit the exact mangled symbol. On macOS, the compiler adds a leading '_'
// to C symbols, so naming the function _ZN6google... produces the symbol
// __ZN6google... which matches what dyld expects.
extern "C" void __attribute__((used))
_ZN6google8protobuf7Message19CopyWithSourceCheckERS1_RKS1_(
    google::protobuf::Message& to,
    const google::protobuf::Message& from) {
  to.CopyFrom(from);
}
