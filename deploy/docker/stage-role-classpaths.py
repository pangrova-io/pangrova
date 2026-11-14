#!/usr/bin/env python3
"""Stage each packaged Maven role's resolved runtime in launch order."""
import argparse
import filecmp
import os
from pathlib import Path
import shutil
from zipfile import ZipFile


ROLES = {
    "standalone": Path("pangrova-standalone"),
    "coordinator": Path("pangrova-server/pangrova-server-coordinator"),
    "worker": Path("pangrova-server/pangrova-server-worker"),
}


MAIN_CLASS = {
    "coordinator": "org/pangrova/server/coordinator/bootstrap/PangrovaCoordinatorMain.class",
    "worker": "org/pangrova/server/worker/bootstrap/PangrovaWorkerMain.class",
    "standalone": "org/pangrova/standalone/PangrovaStandaloneMain.class",
}

EXCLUDED_PACKAGES = {
    "coordinator": ("org/pangrova/worker/", "org/pangrova/server/worker/"),
    "worker": ("org/pangrova/planner/", "org/pangrova/coordinator/", "org/pangrova/server/coordinator/"),
    "standalone": (),
}


def check_contents(role, source, application):
    with ZipFile(source) as jar:
        if source == application and MAIN_CLASS[role] not in jar.namelist():
            raise ValueError(f"Missing {role} process entry point in {source.name}")
        for member in jar.namelist():
            path = member
            if member.startswith("META-INF/versions/"):
                path = member.split("/", 3)[-1]
            if path.endswith(".class") and path.startswith(EXCLUDED_PACKAGES[role]):
                raise ValueError(f"Unexpected {role} runtime class in {source.name}: {member}")
            if role == "coordinator" and path.rsplit("/", 1)[-1] in (
                    "libpangrova_native.so", "libpangrova_native.dylib", "pangrova_native.dll"):
                raise ValueError(f"Unexpected Coordinator native runtime in {source.name}: {member}")


def stage(project, library):
    library.mkdir(parents=True, exist_ok=True)
    for role, module in ROLES.items():
        target = project / module / "target"
        properties = dict(line.split("=", 1) for line in
                          (target / "maven-archiver/pom.properties").read_text().splitlines()
                          if line and not line.startswith("#"))
        application = target / f"{properties['artifactId']}-{properties['version']}.jar"
        entries = (target / "runtime-classpath.txt").read_text().strip().split(os.pathsep)
        role_dir = library / role
        role_dir.mkdir()
        names = []
        for source in [application, *(Path(entry) for entry in entries)]:
            if not source.is_file() or source.suffix != ".jar":
                raise ValueError(f"{role} runtime dependency is missing or is not a JAR: {source}")
            name = source.name
            if name.startswith(("jersey-server-1.", "jersey-servlet-1.", "jersey-guice-1.",
                                "slf4j-log4j12-", "slf4j-reload4j-")):
                continue
            if name in names or any(ch in name for ch in ":\r\n"):
                raise ValueError(f"Ambiguous {role} runtime JAR name: {name}")
            if ((role == "worker" and name.startswith(("pangrova-planner-", "pangrova-server-coordinator-",
                                                      "pangrova-coordination-control-")))
                    or (role == "coordinator" and name.startswith(("pangrova-worker-", "pangrova-server-worker-", "pangrova-native-bundle-")))
                    or name.startswith("pangrova-testing-")):
                raise ValueError(f"Unexpected {role} runtime dependency: {name}")
            check_contents(role, source, application)
            # The frozen regression inventory uses the flat standalone closure.
            flat = library / name
            if role == "standalone":
                shutil.copyfile(source, flat)
            if flat.is_file() and filecmp.cmp(source, flat, shallow=False):
                os.link(flat, role_dir / name)
            else:
                shutil.copyfile(source, role_dir / name)
            names.append(name)
        (role_dir / "classpath.list").write_text("\n".join(names) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("project", type=Path)
    parser.add_argument("library", type=Path)
    args = parser.parse_args()
    stage(args.project, args.library)
