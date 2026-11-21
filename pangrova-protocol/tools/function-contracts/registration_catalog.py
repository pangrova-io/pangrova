"""Deterministic build intermediate for the actual registered signature export.

This is not a SQL capability manifest. Publishing requires canonical binding
declarations, descriptor-checked templates and actual resolver differential proof.
"""

from copy import deepcopy
import hashlib
import json

from registered_signature import exact_bool, exact_keys, normalize_signature
from scalar_implementations import normalize_scalar_implementations


KINDS = {"SCALAR", "AGGREGATE", "WINDOW"}
MAX_JSON_BYTES = 32 * 1024 * 1024
MAX_FUNCTIONS = 65536


def canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False)


def reject_duplicate_fields(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("Duplicate field in registered function export")
        result[name] = value
    return result


def read_export(source):
    if not isinstance(source, str) or len(source.encode("utf-8")) > MAX_JSON_BYTES:
        raise ValueError("Registered export exceeds its byte bound")
    return json.loads(source, object_pairs_hook=reject_duplicate_fields,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Non-finite JSON number")))


def normalize_function(source, special_forms=()):
    exact_keys(source, ("name", "kind", "overloads", "resolutionPolicy"),
               ("aggregateWindow", "windowProcessMode", "distinctImplementation",
                "simpleImplementations", "vectorImplementations"))
    name, kind, overloads = source["name"], source["kind"], source["overloads"]
    if not isinstance(name, str) or not name or kind not in KINDS:
        raise ValueError("Registered function has malformed identity or kind")
    policy = "SCALAR_RUNTIME_PRIORITY" if kind == "SCALAR" else "ORDERED_FIRST_MATCH"
    if source["resolutionPolicy"] != policy:
        raise ValueError("Registered resolver policy does not match the actual function kind")
    if not isinstance(overloads, list) or len(overloads) > MAX_FUNCTIONS:
        raise ValueError("Registered function has no signatures or exceeds its bound")
    if not overloads and not (kind == "SCALAR" and name in special_forms):
        raise ValueError("A registration without signatures must have an actual special-form resolver")
    implementation_fields = {"simpleImplementations", "vectorImplementations"}
    if kind == "SCALAR" and not implementation_fields <= source.keys():
        raise ValueError("Scalar inventory is missing actual implementation selection metadata")
    if kind != "SCALAR" and implementation_fields & source.keys():
        raise ValueError("Scalar implementation selection metadata assigned to another kind")
    if kind == "WINDOW":
        if not {"aggregateWindow", "windowProcessMode"} <= source.keys():
            raise ValueError("Window registration is missing actual process metadata")
        if source["windowProcessMode"] not in ("ROWS", "PARTITION"):
            raise ValueError("Unknown actual window process mode")
        if "distinctImplementation" in source and (not source["aggregateWindow"]
                or not isinstance(source["distinctImplementation"], str)
                or not source["distinctImplementation"] or source["distinctImplementation"] == name):
            raise ValueError("Distinct window dependency must name another actual aggregate-window registration")
    elif {"aggregateWindow", "windowProcessMode", "distinctImplementation"} & source.keys():
        raise ValueError("Window metadata was assigned to a non-window function")
    result = {"name": name, "kind": kind, "resolutionPolicy": policy}
    for flag in ("aggregateWindow",):
        if flag in source:
            result[flag] = exact_bool(source[flag])
    if "windowProcessMode" in source:
        result["windowProcessMode"] = source["windowProcessMode"]
    if "distinctImplementation" in source:
        result["distinctImplementation"] = source["distinctImplementation"]
    normalized = [normalize_signature(overload, kind) for overload in overloads]
    if kind == "SCALAR":
        # This is an inventory, NOT SimpleFunction implementation precedence.
        # Exact output ambiguity requires actual priority/physical metadata or
        # resolver proof; the first sorted signature is never authoritative.
        identities = {canonical_json(overload) for overload in normalized}
        normalized = [json.loads(value) for value in sorted(identities)]
        simple, vector = normalize_scalar_implementations(source)
        implemented = {canonical_json(entry["signature"]) for entry in simple}
        implemented.update(canonical_json(entry["signature"]) for entry in vector)
        if implemented != identities:
            raise ValueError("Logical scalar inventory differs from the actual implementation signatures")
        result.update(simpleImplementations=simple, vectorImplementations=vector)
    result["overloads"] = normalized
    return result


def normalize_catalog(source):
    exact_keys(source, ("schemaVersion", "functions", "specialForms"))
    if type(source["schemaVersion"]) is not int or source["schemaVersion"] != 4:
        raise ValueError("Unknown registered-export schema version")
    functions, special_forms = source["functions"], source["specialForms"]
    if not isinstance(functions, list) or len(functions) > MAX_FUNCTIONS:
        raise ValueError("Registered function inventory exceeds its bound")
    if not isinstance(special_forms, list) or len(special_forms) > MAX_FUNCTIONS:
        raise ValueError("Special-form inventory exceeds its bound")
    normalized, identities = [], set()
    for function in functions:
        entry = normalize_function(function, special_forms)
        identity = (entry["kind"], entry["name"])
        if identity in identities:
            raise ValueError("Duplicate registered function identity")
        identities.add(identity)
        normalized.append(entry)
    by_identity = {(entry["kind"], entry["name"]): entry for entry in normalized}
    for entry in normalized:
        if "distinctImplementation" not in entry:
            continue
        target = by_identity.get(("WINDOW", entry["distinctImplementation"]))
        if (target is None or not target["aggregateWindow"] or target["windowProcessMode"] != "PARTITION"
                or target["overloads"] != entry["overloads"]):
            raise ValueError("Distinct window dependency is absent or has a different actual signature")
    if any(not isinstance(name, str) or not name for name in special_forms):
        raise ValueError("Malformed special-form identity")
    if len(special_forms) != len(set(special_forms)):
        raise ValueError("Duplicate special-form identity")
    result = {"schemaVersion": 4, "functions": sorted(normalized, key=lambda e: (e["kind"], e["name"])),
              "specialForms": sorted(special_forms)}
    return result


def registration_digest(source):
    """Compatibility/provenance digest only; it is not authentication or proof."""
    normalized = normalize_catalog(deepcopy(source))
    return hashlib.sha256(canonical_json(normalized).encode("utf-8")).hexdigest()
