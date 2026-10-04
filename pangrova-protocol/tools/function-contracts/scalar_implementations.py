"""Validate actual resolver snapshots; physical tags remain a build intermediate."""

import re

from registered_signature import MAX_DEPTH, MAX_TYPES, exact_bool, exact_keys, normalize_signature


def normalize_physical_layout(source, depth=0, budget=None):
    budget = budget if budget is not None else [MAX_TYPES]
    budget[0] -= 1
    if depth > MAX_DEPTH or budget[0] < 0:
        raise ValueError("Physical registration layout exceeds its bound")
    exact_keys(source, ("kind", "children"))
    if (not isinstance(source["kind"], str) or not re.fullmatch(r"[A-Z][A-Z0-9_]*", source["kind"])
            or not isinstance(source["children"], list)):
        raise ValueError("Physical layout requires an exact runtime tag and child list")
    return {"kind": source["kind"], "children": [normalize_physical_layout(child, depth + 1, budget)
                                                for child in source["children"]]}


def normalize_scalar_implementations(source):
    simple, vector = source["simpleImplementations"], source["vectorImplementations"]
    if not isinstance(simple, list) or not isinstance(vector, list) or len(simple) + len(vector) > MAX_TYPES:
        raise ValueError("Actual scalar implementations exceed their bound")
    normalized = []
    for entry in simple:
        exact_keys(entry, ("signature", "priority", "argumentsPhysical", "outputPhysical",
                           "deterministic", "defaultNullBehavior"))
        if type(entry["priority"]) is not int or not 0 <= entry["priority"] <= (1 << 32) - 1:
            raise ValueError("Actual scalar priority requires an exact unsigned 32-bit integer")
        signature = normalize_signature(entry["signature"], "SCALAR")
        arguments = entry["argumentsPhysical"]
        if not isinstance(arguments, list) or len(arguments) != len(signature["arguments"]):
            raise ValueError("Actual physical argument arity differs from its logical signature")
        normalized.append({"signature": signature, "priority": entry["priority"],
                           "argumentsPhysical": [normalize_physical_layout(arg) for arg in arguments],
                           "outputPhysical": normalize_physical_layout(entry["outputPhysical"]),
                           "deterministic": exact_bool(entry["deterministic"]),
                           "defaultNullBehavior": exact_bool(entry["defaultNullBehavior"])})
    # Preserve the actual traversal order, including equal-priority entries.
    # Sorting logical signatures is not the runtime's selection algorithm.
    return normalized, [normalize_vector_implementation(entry) for entry in vector]


def normalize_vector_implementation(source):
    fields = ("deterministic", "defaultNullBehavior", "supportsFlattening", "companionFunction")
    exact_keys(source, ("signature", *fields))
    return {"signature": normalize_signature(source["signature"], "SCALAR"),
            **{field: exact_bool(source[field]) for field in fields}}
