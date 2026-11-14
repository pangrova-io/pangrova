#!/usr/bin/env python3
"""Check literal source-local C/C++ include paths and report component edges."""
import argparse
from collections import defaultdict
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
import re
import sys

SOURCE_SUFFIXES = {".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".inc", ".ipp"}
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


def check(source, report=False):
    edges, components = collect(source)
    if report:
        for origin in sorted(components):
            for target, (file, line, header) in sorted(components[origin].items()):
                print(f"{origin} -> {target}: {file}:{line} includes {header}")
    try:
        tuple(TopologicalSorter(edges).static_order())
    except CycleError as error:
        # graphlib reports predecessors before successors. Display include direction.
        cycle = list(reversed(error.args[1]))
        print("Native file include cycle:", file=sys.stderr)
        for file, header in zip(cycle, cycle[1:]):
            print(f"  {file}:{edges[file][header]} includes {header}", file=sys.stderr)
        return 1
    print(f"Native literal include graph: {len(edges)} files, {sum(map(len, edges.values()))} local edges, 0 cycles")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--components", action="store_true", help="print a source witness for each component edge")
    args = parser.parse_args()
    sys.exit(check(args.source, args.components))
