#!/usr/bin/env python3
"""Generate the module fact baseline consumed by ModuleDependencyPropertyTest.

Usage:
    generate-module-baseline.py            # rewrite .pangrova/module-baseline.yaml
    generate-module-baseline.py --check    # fail when the committed baseline is stale

The baseline records facts only (reactor layout, package ownership, current
references, ServiceLoader descriptors, declared POM dependencies). Policy lives in
.pangrova/modules.yaml and is validated against these facts by the Java test.
Requires JDK 17: source-launches Testing Kit's shared JavaSourceReferences parser
without a Maven build. The YAML `imports` field includes qualified code references.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import subprocess
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE = ROOT / ".pangrova" / "module-baseline.yaml"
NS = {"m": "http://maven.apache.org/POM/4.0.0"}

SCOPE_PRECEDENCE = {"compile": 4, "runtime": 3, "provided": 2, "test": 1}


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def pom_artifacts() -> dict[str, dict]:
    artifacts: dict[str, dict] = {}
    def fail_walk(error: OSError) -> None:
        raise error

    poms = []
    for directory, children, files in os.walk(ROOT, onerror=fail_walk):
        children[:] = sorted(name for name in children if name not in {"target", ".git", "node_modules"})
        if "pom.xml" in files:
            poms.append(Path(directory) / "pom.xml")
    for pom in sorted(poms):
        tree = ET.parse(pom)
        root = tree.getroot()
        artifact_id = root.findtext("m:artifactId", namespaces=NS)
        if artifact_id and artifact_id.startswith("pangrova-") and artifact_id.endswith(
                ("-core", "-spi", "-adapter", "-bridge")):
            raise ValueError(f"{rel(pom)}: name {artifact_id} for its capability or implementation")
        if artifact_id in artifacts:
            raise ValueError(f"Duplicate reactor artifact {artifact_id}: "
                             f"{artifacts[artifact_id]['pom']} and {rel(pom)}")
        packaging = root.findtext("m:packaging", namespaces=NS) or "jar"
        children = root.findall("m:modules/m:module", NS)
        if packaging == "pom" and pom.parent != ROOT and len(children) == 1:
            raise ValueError(f"{rel(pom)} aggregates only {children[0].text}; flatten the family")
        has_sources = any((pom.parent / s).is_dir() for s in ("src/main/java", "src/test/java"))
        deps = {}
        for dep in root.findall("m:dependencies/m:dependency", NS):
            group = dep.findtext("m:groupId", namespaces=NS) or ""
            if "pangrova" not in group and group != "org.pangrova":
                continue
            target = dep.findtext("m:artifactId", namespaces=NS)
            scope = dep.findtext("m:scope", namespaces=NS) or "compile"
            if target not in deps or SCOPE_PRECEDENCE.get(scope, 1) >= SCOPE_PRECEDENCE.get(deps[target], 1):
                deps[target] = scope
        artifacts[artifact_id] = {
            "path": rel(pom.parent) if rel(pom.parent) != "." else ".",
            "pom": rel(pom),
            "packaging": packaging,
            "leaf": has_sources or packaging != "pom",
            "dependencies": deps,
        }
    return artifacts


SCANNER = ROOT / ("pangrova-testing/pangrova-testing-kit/src/main/java/"
                  "org/pangrova/testing/testkit/architecture/JavaSourceReferences.java")


def source_facts(roots: list[Path]) -> list[dict]:
    """Use exactly the JDK AST scanner used by the Java architecture gate."""
    java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if os.environ.get("JAVA_HOME") else "java"
    try:
        result = subprocess.run([java, "-Dfile.encoding=UTF-8", str(SCANNER), *map(str, roots)],
                                text=True, encoding="utf-8", capture_output=True, check=True)
    except subprocess.CalledProcessError as failure:
        raise RuntimeError("Java source scan failed:\n" + failure.stderr) from failure
    facts = [{"packages": set(), "references": set()} for _ in roots]
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) not in (3, 4) or parts[0] not in ("P", "R"):
            raise ValueError(f"Invalid Java source fact: {line!r}")
        kind, index, name = parts[:3]
        if (kind == "P" and len(parts) != 3) or (kind == "R" and len(parts) != 4):
            raise ValueError(f"Invalid Java source fact: {line!r}")
        if not 0 <= int(index) < len(roots):
            raise ValueError(f"Invalid Java source root index: {index}")
        facts[int(index)]["packages" if kind == "P" else "references"].add(name)
    return facts


def collect_descriptors(module_dir: Path) -> dict[str, list[str]]:
    descriptors: dict[str, list[str]] = {}
    service_dir = module_dir / "src/main/resources/META-INF/services"
    if not service_dir.is_dir():
        return descriptors
    for descriptor in sorted(service_dir.iterdir()):
        if not descriptor.is_file() or descriptor.name.startswith("."):
            continue
        providers = [
            line.strip()
            for line in descriptor.read_text().splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        descriptors[descriptor.name] = providers
    return descriptors


def merge_witness(artifacts: dict, groups: list[list[str]]) -> list[tuple[str, str]]:
    """Return original POM edges forming a cycle through a proposed merged artifact."""
    owners = {name: (name,) for name in artifacts}
    merged = []
    for names in groups:
        group = tuple(sorted(set(names)))
        if len(group) < 2:
            raise ValueError("Select at least two distinct artifacts in each merge group")
        for name in group:
            if name not in artifacts or not artifacts[name]["leaf"]:
                raise ValueError(f"Unknown or non-leaf artifact: {name}")
            if len(owners[name]) > 1:
                raise ValueError(f"Artifact occurs in multiple merge groups: {name}")
            owners[name] = group
        merged.append(group)

    # Keep one real edge per pair of contracted nodes for actionable diagnostics.
    graph = defaultdict(dict)
    for source in sorted(artifacts):
        for target in sorted(artifacts[source]["dependencies"]):
            if target in owners and owners[source] != owners[target]:
                graph[owners[source]].setdefault(owners[target], (source, target))

    for start in merged:
        queue = deque([start])
        parents = {start: None}
        while queue:
            source = queue.popleft()
            for target, edge in graph[source].items():
                if target == start:
                    path = [edge]
                    while source != start:
                        previous = parents[source]
                        path.append(graph[previous][source])
                        source = previous
                    return list(reversed(path))
                if target not in parents:
                    parents[target] = source
                    queue.append(target)
    return []


def check_merge(groups: list[list[str]]) -> int:
    artifacts = pom_artifacts()
    try:
        edges = merge_witness(artifacts, groups)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    if edges:
        print("Merging these artifacts closes a declared POM dependency cycle:")
        for group in groups:
            print("  merge: " + ", ".join(group))
        for source, target in edges:
            print(f"  {source} -> {target} ({artifacts[source]['dependencies'][target]}, {artifacts[source]['pom']})")
        return 1
    print("No declared POM dependency cycle passes through a proposed merged artifact.")
    return 0


def export_graphs(directory: Path, leaves: dict, main_packages: dict) -> None:
    """Write deterministic DOT graphs of declared POM edges and production package ownership."""
    directory.mkdir(parents=True, exist_ok=True)
    dependencies = ['digraph dependencies {', '  rankdir=LR;']
    ownership = ['digraph ownership {', '  rankdir=LR;']
    for artifact, metadata in sorted(leaves.items()):
        node = json.dumps(artifact)
        dependencies.append(f'  {node};')
        ownership.append(f'  {node} [shape=box];')
        for target, scope in sorted(metadata["dependencies"].items()):
            if target in leaves:
                dependencies.append(f'  {node} -> {json.dumps(target)} [label={json.dumps(scope)}];')
        for package in sorted(main_packages[artifact]):
            ownership.append(f'  {node} -> {json.dumps(package)};')
    for name, lines in (("module-dependencies", dependencies), ("package-ownership", ownership)):
        (directory / f"{name}.dot").write_text("\n".join([*lines, "}", ""]), encoding="utf-8")


def main(check: bool = False, graph_dir: Path | None = None) -> int:
    artifacts = pom_artifacts()
    leaves = {name: meta for name, meta in artifacts.items() if meta["leaf"]}
    aggregators = {name: meta for name, meta in artifacts.items() if not meta["leaf"]}

    packages: dict[str, set[str]] = {}
    main_packages: dict[str, set[str]] = {}
    imports: dict[str, dict[str, set[str]]] = {}
    descriptors: dict[str, dict[str, list[str]]] = {}
    source_sets = [(name, kind, ROOT / meta["path"] / "src" / kind / "java")
                   for name, meta in leaves.items() for kind in ("main", "test")]
    scanned = source_facts([source for _, _, source in source_sets])
    for (name, kind, _), source in zip(source_sets, scanned):
        packages.setdefault(name, set()).update(source["packages"])
        if kind == "main":
            main_packages[name] = source["packages"]
        imports.setdefault(name, {})[kind] = source["references"]
    for name, meta in leaves.items():
        descriptors[name] = collect_descriptors(ROOT / meta["path"])

    # Longest-prefix ownership across all declared packages.
    owner: dict[str, str] = {}
    for name, pkgs in sorted(packages.items()):
        for package in sorted(pkgs):
            if package in owner:
                raise ValueError(f"Split package {package}: {owner[package]} and {name}")
            owner[package] = name

    def package_of(fqcn: str) -> str | None:
        """Resolve a type (possibly nested) to its longest declared package prefix."""
        parts = fqcn.split(".")
        for index in range(len(parts), 0, -1):
            candidate = ".".join(parts[:index])
            if candidate in owner:
                return candidate
        return None

    def owner_of(package: str) -> str | None:
        return owner.get(package)

    edges: dict[str, dict[str, set[str]]] = {"main": defaultdict(set), "test": defaultdict(set)}
    exported: dict[str, set[str]] = {name: set() for name in leaves}
    unresolved: dict[str, set[str]] = {"main": set(), "test": set()}
    for name, kinds in imports.items():
        for kind, imported in kinds.items():
            for fqcn in imported:
                package = package_of(fqcn)
                if package is None:
                    unresolved[kind].add(fqcn)
                    continue
                target = owner_of(package)
                if target == name:
                    continue
                edges[kind][name].add(target)
                exported[target].add(package)

    lines: list[str] = []
    lines.append("# GENERATED by .pangrova/scripts/generate-module-baseline.py. Do not edit.")
    lines.append("# Regenerate with: python3 .pangrova/scripts/generate-module-baseline.py")
    lines.append("version: 1")
    lines.append("reactor:")
    for name in sorted(aggregators):
        lines.append(f"  - artifactId: {name}")
        lines.append(f"    path: {aggregators[name]['path']}")
    lines.append("artifacts:")
    for name in sorted(leaves):
        meta = leaves[name]
        lines.append(f"  {name}:")
        lines.append(f"    path: {meta['path']}")
        lines.append("    mainPackages:")
        for package in sorted(main_packages[name]):
            lines.append(f"      - {package}")
        if meta["dependencies"]:
            lines.append("    pomDependencies:")
            for target in sorted(meta["dependencies"]):
                lines.append(f"      {target}: {meta['dependencies'][target]}")
        else:
            lines.append("    pomDependencies: []")
        lines.append("    imports:")
        for kind in ("main", "test"):
            if edges[kind].get(name):
                lines.append(f"      {kind}:")
                for target in sorted(edges[kind][name]):
                    lines.append(f"        - {target}")
            else:
                lines.append(f"      {kind}: []")
        if exported[name]:
            lines.append("    exportedPackages:")
            for package in sorted(exported[name]):
                lines.append(f"      - {package}")
        else:
            lines.append("    exportedPackages: []")
        lines.append("    providers:")
        if descriptors[name]:
            for service in sorted(descriptors[name]):
                lines.append(f"      \"{service}\":")
                for provider in descriptors[name][service]:
                    lines.append(f"        - {provider}")
        else:
            lines.append("      {}")
    lines.append("unresolvedImports:")
    for kind in ("main", "test"):
        lines.append(f"  {kind}:")
        if unresolved[kind]:
            for fqcn in sorted(unresolved[kind]):
                lines.append(f"    - {fqcn}")
        else:
            lines.append("    []")

    rendered = "\n".join(lines) + "\n"
    if graph_dir is not None:
        export_graphs(graph_dir, leaves, main_packages)
        if not check:
            return 0
    if check:
        current = BASELINE.read_text() if BASELINE.exists() else ""
        if current != rendered:
            print("module baseline is stale; regenerate with "
                  ".pangrova/scripts/generate-module-baseline.py", file=sys.stderr)
            return 1
        print("module baseline is current")
        return 0

    BASELINE.write_text(rendered)
    print(f"wrote {rel(BASELINE)} ({len(leaves)} code/resource artifacts, {len(aggregators)} aggregators)")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check the existing fact baseline")
    parser.add_argument("--graph-dir", type=Path, help="export graphs without rewriting the baseline")
    parser.add_argument("--check-merge", nargs="+", action="append", metavar="ARTIFACT",
                        help="simulate merging leaf artifacts using declared POM edges; repeat for simultaneous merges")
    options = parser.parse_args()
    if options.check_merge:
        if options.check or options.graph_dir:
            parser.error("--check-merge is a standalone inspection")
        raise SystemExit(check_merge(options.check_merge))
    raise SystemExit(main(options.check, options.graph_dir))
