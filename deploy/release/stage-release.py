#!/usr/bin/env python3
"""Validate downloaded matrix artifacts and assemble host distributions."""

import argparse
from dataclasses import dataclass
import gzip
import hashlib
import json
from pathlib import Path
import re
import runpy
import shutil
import struct
import tarfile
import tempfile
from zipfile import ZipFile


@dataclass(frozen=True)
class Platform:
    os: str
    arch: str
    extension: str
    machine: int

    @property
    def library(self):
        return f"libpangrova_native.{self.extension}"

    @property
    def resource(self):
        return f"META-INF/native/{self.os}/{self.arch}/{self.library}"


PLATFORMS = {
    "linux-x86_64": Platform("linux", "x86_64", "so", 62),
    "linux-aarch_64": Platform("linux", "aarch64", "so", 183),
    "osx-x86_64": Platform("osx", "x86_64", "dylib", 0x01000007),
    "osx-aarch_64": Platform("osx", "aarch64", "dylib", 0x0100000C),
}
HOST_DISTRIBUTIONS = {"linux-x86_64"}


def checksum(file):
    digest = hashlib.sha256()
    for block in iter(lambda: file.read(1024 * 1024), b""):
        digest.update(block)
    return digest.hexdigest()


def file_checksum(path):
    with path.open("rb") as file:
        return checksum(file)


def check_architecture(path, platform):
    with path.open("rb") as file:
        header = file.read(64)
    if platform.os == "linux":
        # ELF64 little endian, ET_DYN, followed by the machine number.
        valid = (len(header) == 64 and header[:7] == b"\x7fELF\x02\x01\x01"
                 and struct.unpack_from("<HH", header, 16) == (3, platform.machine))
    else:
        # Thin little-endian Mach-O64, followed by CPU type and MH_DYLIB.
        valid = (len(header) >= 32 and header[:4] == b"\xcf\xfa\xed\xfe"
                 and struct.unpack_from("<I", header, 4)[0] == platform.machine
                 and struct.unpack_from("<I", header, 12)[0] == 6)
    if not valid:
        raise ValueError(f"{path} is not a {platform.os}/{platform.arch} shared library")


def single_file(directory, pattern):
    entries = list(directory.iterdir()) if directory.is_dir() else []
    if len(entries) != 1 or not entries[0].is_file() or not entries[0].match(pattern):
        raise ValueError(f"Expected exactly one {pattern} in {directory}")
    return entries[0]


def validate_matrix(artifacts, classifiers):
    expected = {f"release-native-{kind}-{classifier}" for classifier in classifiers
                for kind in ("bundle", "lib")}
    actual = {path.name for path in artifacts.glob("release-native-*")}
    if actual != expected:
        raise ValueError(f"Native matrix mismatch. Missing {sorted(expected - actual)}. "
                         f"Unexpected {sorted(actual - expected)}.")
    result = {}
    for classifier in classifiers:
        platform = PLATFORMS[classifier]
        raw = single_file(artifacts / f"release-native-lib-{classifier}", platform.library)
        bundle = single_file(artifacts / f"release-native-bundle-{classifier}",
                             f"pangrova-native-bundle-*-{classifier}.jar")
        check_architecture(raw, platform)
        digest = file_checksum(raw)
        with ZipFile(bundle) as jar:
            native_files = [info.filename for info in jar.infolist()
                            if not info.is_dir() and info.filename.startswith("META-INF/native/")]
            if native_files != [platform.resource]:
                raise ValueError(f"Wrong or duplicate JNI resources in {bundle}")
            with jar.open(platform.resource) as native:
                if checksum(native) != digest:
                    raise ValueError(f"JNI bytes in {bundle} differ from {raw}")
        result[classifier] = (raw, bundle, digest)
    return result


def stage_runtime(project, base):
    stager = runpy.run_path(str(project / "deploy/docker/stage-role-classpaths.py"))
    stager["stage"](project, base / "lib")
    for directory in ("bin", "etc"):
        shutil.copytree(project / directory, base / directory, ignore=shutil.ignore_patterns("*.local"))
    cli = list((project / "pangrova-client/pangrova-client-cli/target").glob("pangrova-client-cli-*-exec.jar"))
    if len(cli) != 1:
        raise ValueError("Expected one packaged CLI executable")
    (base / "cli").mkdir()
    shutil.copy2(cli[0], base / "cli" / cli[0].name)
    for role in ("worker", "standalone", "coordinator"):
        role_dir = base / "lib" / role
        for jar_path in role_dir.glob("pangrova-*.jar"):
            with ZipFile(jar_path) as jar:
                if any(name.startswith("META-INF/native/") and Path(name).name.startswith("libpangrova_native.")
                       for name in jar.namelist()):
                    raise ValueError(f"Remove staged JNI resources from the neutral Maven build: {jar_path.name}")
    shutil.copytree(project / "deploy/release/preview", base / "preview",
                    ignore=shutil.ignore_patterns("__pycache__"))
    for name in ("iceberg-smoke.py", "iceberg-smoke.sql"):
        (base / "examples").mkdir(exist_ok=True)
        shutil.copy2(project / "examples" / name, base / "examples" / name)


def archive_distribution(base, output, name, manifest, bundle):
    def normalize(info):
        info.uid = info.gid = info.mtime = 0
        info.uname = info.gname = ""
        return info

    with (output / f"{name}.tar.gz").open("wb") as dest:
        with gzip.GzipFile(filename="", mode="wb", fileobj=dest, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as tar:
                for directory in ("bin", "etc", "cli", "examples", "preview"):
                    tar.add(base / directory, f"{name}/{directory}", filter=normalize)
                tar.add(manifest, f"{name}/manifest.json", filter=normalize)
                for role in ("coordinator", "worker", "standalone"):
                    role_dir = base / "lib" / role
                    archive_dir = f"{name}/lib/{role}"
                    tar.add(role_dir, archive_dir, filter=lambda info: (
                        None if info.name == f"{archive_dir}/classpath.list" else normalize(info)))
                    if role != "coordinator":
                        tar.add(bundle, f"{archive_dir}/{bundle.name}", filter=normalize)
                    lines = (role_dir / "classpath.list").read_text(encoding="utf-8")
                    if role != "coordinator":
                        lines += bundle.name + "\n"
                    with tempfile.TemporaryFile() as data:
                        data.write(lines.encode("utf-8"))
                        data.seek(0)
                        info = tarfile.TarInfo(f"{archive_dir}/classpath.list")
                        info.size = len(lines.encode("utf-8"))
                        tar.addfile(info, data)


def stage_release(project, artifacts, output, version, revision, classifiers):
    if not re.fullmatch(r"[0-9][A-Za-z0-9._+-]*", version):
        raise ValueError("Invalid release version")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Expected the full release source commit")
    if len(set(classifiers)) != len(classifiers):
        raise ValueError("Duplicate release classifier")
    matrix = validate_matrix(artifacts, classifiers)
    with tempfile.TemporaryDirectory(dir=output.parent) as temporary:
        work = Path(temporary)
        staged = work / "release"
        staged.mkdir()
        base = work / "runtime"
        if HOST_DISTRIBUTIONS.intersection(classifiers):
            stage_runtime(project, base)
        for classifier, (raw, bundle, digest) in matrix.items():
            raw_name = f"libpangrova_native-{classifier}.{PLATFORMS[classifier].extension}"
            shutil.copyfile(raw, staged / raw_name)
            shutil.copyfile(bundle, staged / bundle.name)
            if classifier not in HOST_DISTRIBUTIONS:
                continue
            manifest = work / "manifest.json"
            manifest.write_text(json.dumps({
                "version": version, "sourceRevision": revision, "classifier": classifier,
                "nativeBundle": bundle.name, "nativeResource": PLATFORMS[classifier].resource,
                "nativeSha256": digest,
            }, indent=2) + "\n", encoding="utf-8")
            archive_distribution(base, staged, f"pangrova-{version}-{classifier}", manifest, bundle)
        if HOST_DISTRIBUTIONS.intersection(classifiers):
            shutil.copy2(project / "deploy/release/pangrova-preview.sh", staged / "pangrova-preview.sh")
        lines = "".join(f"{file_checksum(path)}  {path.name}\n" for path in sorted(staged.iterdir()))
        (staged / "SHA256SUMS").write_text(lines, encoding="utf-8")
        if output.exists():
            raise FileExistsError(f"Release output already exists: {output}")
        staged.rename(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project", type=Path)
    parser.add_argument("artifacts", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--platform", choices=PLATFORMS, action="append")
    args = parser.parse_args()
    stage_release(args.project.resolve(), args.artifacts, args.output, args.version, args.revision,
                  args.platform or PLATFORMS)
