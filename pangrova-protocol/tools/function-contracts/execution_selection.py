"""One bounded control contract for the actual execution registry entries."""

import hashlib

from registered_signature import exact_keys
from registration_catalog import canonical_json, read_export
from special_type_programs import validate_programs


FAMILIES = {"SPECIAL_FORM", "VECTOR", "SCALAR"}
SELECTIONS = {"FIRST_BOUND", "MIN_PRIORITY_FIRST_BOUND"}


def read_selection(source):
    value = read_export(source)
    exact_keys(value, ("schemaVersion", "families", "coercion", "matching", "constantValidation",
                       "factoryInvocation", "initializationFailure", "specialTypePrograms"))
    if type(value["schemaVersion"]) is not int or value["schemaVersion"] != 2:
        raise ValueError("Unknown execution-selection contract version")
    families = value["families"]
    if not isinstance(families, list) or len(families) != len(FAMILIES):
        raise ValueError("Execution selection requires each actual registry family once")
    observed = set()
    for step in families:
        exact_keys(step, ("family", "selection"))
        if step["family"] not in FAMILIES or step["family"] in observed:
            raise ValueError("Unknown or duplicate execution-selection family")
        if step["selection"] not in SELECTIONS:
            raise ValueError("Unknown execution-selection operation")
        observed.add(step["family"])
    policies = {"coercion": "FORBIDDEN", "constantValidation": "AFTER_SELECTION",
                "factoryInvocation": "EXECUTION_ONLY",
                "initializationFailure": "PRESERVE_SELECTED_IMPLEMENTATION"}
    if any(value[key] != expected for key, expected in policies.items()):
        raise ValueError("Execution-selection contract changes a forbidden runtime boundary")
    if value["matching"] != ["SIGNATURE", "ARGUMENT_PHYSICAL", "RESULT_PHYSICAL"]:
        raise ValueError("Execution selection must preserve all actual signature constraints")
    validate_programs(value["specialTypePrograms"])
    return value


def selection_digest(value):
    """Content identity, not authentication or a claim about runtime behavior."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def select_bound(value, entries, bind):
    """Reference control evaluation; binding and values are not implemented here."""
    if any(entry["family"] not in FAMILIES for entry in entries):
        raise ValueError("Unknown actual implementation family")
    for step in value["families"]:
        selected, priority = None, None
        for entry in entries:
            if entry["family"] != step["family"]:
                continue
            result = bind(entry)
            if result is None:
                continue
            if selected is None or entry["priority"] < priority:
                selected, priority = result, entry["priority"]
            if step["selection"] == "FIRST_BOUND":
                return selected
        if selected is not None:
            return selected
    return None
