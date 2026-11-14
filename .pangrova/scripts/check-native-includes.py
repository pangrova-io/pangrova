#!/usr/bin/env python3
"""Check literal source-local C/C++ include paths and report component edges."""
import argparse
from collections import defaultdict
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
import re
import sys

SOURCE_SUFFIXES = {".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".inc", ".ipp"}
# Existing reciprocal dependencies between Native OBJECT groups. Remove the
# corresponding entry when a dependency is severed. New acyclic edges are free
# to form between independent components.
EXISTING_CYCLIC_EDGES = {
    "capabilities": {"engine", "indexing", "vector"},
    "command": {"capabilities", "engine", "jni", "lance", "udf", "vector"},
    "engine": {"capabilities", "indexing", "jni", "lance", "udf", "vector"},
    "indexing": {"capabilities", "engine"},
    "jni": {"capabilities", "command", "engine", "indexing", "udf"},
    "lance": {"capabilities", "engine"},
    "udf": {"engine", "jni"},
    "vector": {"capabilities", "engine"},
}
# Quoted text shields comment delimiters. Hide multiline raw literals and
# comments while retaining line numbers for diagnostics.
TOKENS = re.compile(
    r'(?P<raw>R"(?P<delimiter>[^ ()\\\t\r\n]{0,16})\(.*?\)(?P=delimiter)")'
    r'|(?P<comment>/\*.*?\*/|//[^\n]*)'
    r'|"(?:\\.|[^"\\\n])*"|\'(?:\\.|[^\'\\\n])*\'',
    re.DOTALL,
)
INCLUDE = re.compile(r'^\s*#\s*include\s*(?:"([^"]+)"|<([^>]+)>)')


def collect(source):
    files = sorted(p for p in source.rglob("*") if p.is_file() and p.suffix in SOURCE_SUFFIXES)
    if not files:
        raise ValueError(f"No C/C++ sources in {source}")
    names = {p.resolve(): p.relative_to(source) for p in files}
    edges = {name: {} for name in names.values()}
    components = defaultdict(dict)
    for path in files:
        owner = path.relative_to(source)
        content = TOKENS.sub(lambda token: " " + "\n" * token[0].count("\n")
                             if token["comment"] or token["raw"] else token[0], path.read_text(encoding="utf-8"))
        logical = ""
        for number, line in enumerate(content.splitlines(), start=1):
            if not logical:
                start = number
            logical += line
            if logical.endswith("\\"):
                logical = logical[:-1]
                continue
            directive = INCLUDE.match(logical)
            logical = ""
            if not directive:
                continue
            quoted, angled = directive.groups()
            candidates = (path.parent / quoted, source / quoted) if quoted else (source / angled,)
            for candidate in candidates:
                target = names.get(candidate.resolve())
                if target is not None:
                    edges[owner].setdefault(target, start)
                    if owner.parts[0] != target.parts[0]:
                        components[owner.parts[0]].setdefault(target.parts[0], (owner, start, target))
                    break
    return edges, components


def cyclic_components(components):
    """Group mutually reachable source directories. This graph is small (one node per component)."""
    reachable = {}
    for origin in components:
        visited = set()
        pending = [origin]
        while pending:
            current = pending.pop()
            if current not in visited:
                visited.add(current)
                pending.extend(components.get(current, ()))
        reachable[origin] = visited
    groups = set()
    for origin, targets in reachable.items():
        members = tuple(sorted(target for target in targets if origin in reachable.get(target, ())))
        if len(members) > 1:
            groups.add(members)
    return sorted(groups)


def check(source, report=False, verify_baseline=False):
    edges, components = collect(source)
    groups = cyclic_components(components)
    if report:
        for origin in sorted(components):
            for target, (file, line, header) in sorted(components[origin].items()):
                print(f"{origin} -> {target}: {file}:{line} includes {header}")
        print(f"Cyclic source-component groups: {len(groups)}")
        for group in groups:
            print("  " + ", ".join(group))
    try:
        tuple(TopologicalSorter(edges).static_order())
    except CycleError as error:
        # graphlib reports predecessors before successors. Display include direction.
        cycle = list(reversed(error.args[1]))
        print("Native file include cycle:", file=sys.stderr)
        for file, header in zip(cycle, cycle[1:]):
            print(f"  {file}:{edges[file][header]} includes {header}", file=sys.stderr)
        return 1
    for origin, target in (("engine", "command"), ("lance", "command"), ("lance", "jni"), ("udf", "command")):
        witness = components.get(origin, {}).get(target)
        if witness:
            file, line, header = witness
            print(f"Forbidden Native dependency {origin} -> {target}: {file}:{line} includes {header}", file=sys.stderr)
            return 1
    observed = set()
    for group in groups:
        for origin in group:
            for target, (file, line, header) in components[origin].items():
                if target not in group:
                    continue
                observed.add((origin, target))
                if target not in EXISTING_CYCLIC_EDGES.get(origin, ()):
                    print(f"New cyclic Native component edge {origin} -> {target}: "
                          f"{file}:{line} includes {header}", file=sys.stderr)
                    print(f"Mutually reachable components: {', '.join(group)}", file=sys.stderr)
                    return 1
    if verify_baseline:
        known = {(origin, target) for origin, targets in EXISTING_CYCLIC_EDGES.items() for target in targets}
        retired = sorted(known - observed)
        if retired:
            for origin, target in retired:
                print(f"Remove resolved cyclic edge from the Native baseline: {origin} -> {target}", file=sys.stderr)
            return 1
    print(f"Native literal include graph: {len(edges)} files, {sum(map(len, edges.values()))} local edges, 0 cycles")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--components", action="store_true", help="print a source witness for each component edge")
    parser.add_argument("--verify-component-baseline", action="store_true",
                        help="require every recorded cyclic edge to still exist in this complete source tree")
    args = parser.parse_args()
    sys.exit(check(args.source, args.components, args.verify_component_baseline))
