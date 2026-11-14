#!/usr/bin/env python3
"""Compare production Java source declarations with a Git revision and export the review inventory."""
import argparse
import csv
import io
import os
import struct
import subprocess
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

SOURCE = "/src/main/java/"
SCANNER = Path(__file__).resolve().parents[2] / (
    "pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/architecture/JavaTypeInventory.java")
NAMESPACE = "{http://maven.apache.org/POM/4.0.0}"


def git(root, *args, data=None):
    return subprocess.run(["git", *args], cwd=root, input=data, stdout=subprocess.PIPE, check=True).stdout


def read_objects(root, revision):
    """Use one Git object batch so the review never needs to check out or stage user files."""
    paths = []
    object_ids = []
    for entry in git(root, "ls-tree", "-rz", revision).split(b"\0"):
        if not entry:
            continue
        metadata, raw_path = entry.split(b"\t", 1)
        path = raw_path.decode("utf-8")
        if path.endswith("/pom.xml") or (SOURCE in path and path.endswith(".java")):
            paths.append(path)
            object_ids.append(metadata.split()[2])
    batch = io.BytesIO(git(root, "cat-file", "--batch", data=b"\n".join(object_ids) + b"\n"))
    result = {}
    for path, object_id in zip(paths, object_ids):
        oid, kind, length = batch.readline().split()
        if oid != object_id or kind != b"blob":
            raise ValueError(f"Cannot read Git blob for {path}")
        result[path] = batch.read(int(length)).decode("utf-8")
        if batch.read(1) != b"\n":
            raise ValueError(f"Incomplete Git blob for {path}")
    return result


def working_files(root):
    paths = git(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard").split(b"\0")
    files = {}
    for raw in paths:
        name = raw.decode("utf-8")
        if not (name.endswith("/pom.xml") or (SOURCE in name and name.endswith(".java"))):
            continue
        path = root / name
        if path.is_file():
            files[name] = path.read_text(encoding="utf-8")
    return files


def declarations(files):
    sources = sorted(path for path in files if SOURCE in path and path.endswith(".java"))
    owners = {path: ET.fromstring(source).findtext(NAMESPACE + "artifactId")
              for path, source in files.items() if path.endswith("/pom.xml")}
    data = io.BytesIO()
    data.write(struct.pack(">i", len(sources)))
    for path in sources:
        source = files[path].encode("utf-8")
        data.write(struct.pack(">i", len(source)))
        data.write(source)
    java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if os.environ.get("JAVA_HOME") else "java"
    parsed = subprocess.run([java, "-Dfile.encoding=UTF-8", str(SCANNER)], input=data.getvalue(),
                            stdout=subprocess.PIPE, check=True)
    result = {}
    for line in parsed.stdout.decode("utf-8").splitlines():
        index, name = line.split("\t")
        path = sources[int(index)]
        artifact = owners[path.split(SOURCE)[0] + "/pom.xml"]
        if not artifact:
            raise ValueError(f"Missing artifact for {path}")
        if name in result:
            raise ValueError(f"Duplicate source owner for {name}: {result[name][1]} and {path}")
        result[name] = (artifact, path)
    return result


def rows(before, after):
    """Exact names have provenance. Simple-name matches are explicitly review candidates."""
    removed = before.keys() - after.keys()
    added = after.keys() - before.keys()
    additions = defaultdict(list)
    removals = defaultdict(list)
    for name in sorted(added):
        additions[name.rsplit(".", 1)[1]].append(name)
    for name in sorted(removed):
        removals[name.rsplit(".", 1)[1]].append(name)

    for name in sorted(before):
        if name in after:
            status = "same owner" if before[name] == after[name] else "same FQN, changed path or artifact"
            yield (name, *before[name], name, *after[name], status, "")
        else:
            matches = additions[name.rsplit(".", 1)[1]]
            yield (name, *before[name], "", "", "", "removed FQN", " ".join(matches))
    for name in sorted(added):
        yield ("", "", "", name, *after[name], "new FQN", " ".join(removals[name.rsplit(".", 1)[1]]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="Git commit before the reviewed refactor")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    revision = git(root, "rev-parse", "--verify", "--end-of-options", args.base + "^{commit}").decode().strip()
    before = declarations(read_objects(root, revision))
    after = declarations(working_files(root))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as output:
        table = csv.writer(output, delimiter="\t", lineterminator="\n")
        table.writerow(("base_commit", "old_fqn", "old_artifact", "old_path", "current_fqn",
                        "current_artifact", "current_path", "structural_status", "same_name_review_candidates"))
        table.writerows((revision, *row) for row in rows(before, after))
    print(f"{len(before)} baseline and {len(after)} current named Java declarations indexed at {args.output}")


if __name__ == "__main__":
    main()
