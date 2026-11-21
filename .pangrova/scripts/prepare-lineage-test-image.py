#!/usr/bin/env python3
"""Run the pinned, unmodified Marquez application on a native Java 17 runtime."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import uuid


SOURCE_IMAGE = "marquezproject/marquez@sha256:6d8a2c2cdb28a4f6d13f9ca6f8cc055d8312c4dee204ebeb848efdd76e6ab30e"
APPLICATION_SHA256 = "6b5a7ace21b6e3b0d98458abacf93a6dfbe55cd1cb784f1f22a8133d6a966c6a"
LABEL_PREFIX = "org.pangrova.lineage-fixture."


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True, timeout=600).strip()


def native_architecture(value):
    aliases = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64", "amd64": "amd64"}
    if value not in aliases:
        raise ValueError(f"Unsupported Docker architecture: {value}")
    return aliases[value]


def verify_application(directory):
    jar = directory / "marquez-api-0.50.0.jar"
    checksum = hashlib.sha256(jar.read_bytes()).hexdigest()
    if checksum != APPLICATION_SHA256:
        raise ValueError("Marquez application differs from the pinned upstream artifact")
    if not (directory / "entrypoint.sh").is_file():
        raise ValueError("Upstream Marquez entrypoint is missing")
    return checksum


def prepare(runtime_image):
    architecture = native_architecture(docker("info", "--format", "{{.Architecture}}"))
    subprocess.run(["docker", "pull", "--platform", "linux/amd64", SOURCE_IMAGE], check=True)
    subprocess.run(["docker", "pull", "--platform", "linux/" + architecture, runtime_image], check=True)
    runtime = json.loads(docker("image", "inspect", runtime_image))[0]
    if native_architecture(runtime["Architecture"]) != architecture:
        raise ValueError("Java runtime image does not match the Docker daemon architecture")
    # Both containers remain stopped; no source architecture code executes during preparation.
    source = docker("create", "--platform", "linux/amd64", "--entrypoint", "/bin/true", SOURCE_IMAGE)
    target = None
    try:
        with tempfile.TemporaryDirectory(prefix="pangrova-lineage-image-") as temporary:
            bundle = Path(temporary)
            app = bundle / "usr" / "src" / "app"
            app.mkdir(parents=True)
            docker("cp", source + ":/usr/src/app/.", str(app))
            checksum = verify_application(app)
            target = docker("create", "--name", "pangrova-lineage-image-" + uuid.uuid4().hex,
                            "--entrypoint", "/bin/true", runtime["Id"])
            docker("cp", str(bundle) + "/.", target + ":/")
            labels = {"source": SOURCE_IMAGE, "application-sha256": checksum,
                      "runtime-image-id": runtime["Id"], "architecture": architecture}
            changes = ["--change", "WORKDIR /usr/src/app", "--change",
                       'ENTRYPOINT ["/usr/src/app/entrypoint.sh"]', "--change", "CMD []"]
            for key, value in labels.items():
                changes.extend(["--change", "LABEL " + LABEL_PREFIX + key + "=" + value])
            image = docker("commit", *changes, target)
            tag = "pangrova-test/marquez:0.50.0-" + architecture + "-" + image.removeprefix("sha256:")[:16]
            docker("tag", image, tag)
            return {"image": tag, "image_id": image, **labels}
    finally:
        if target is not None:
            docker("rm", target)
        docker("rm", source)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-image", default="eclipse-temurin:17-jre",
                        help="Java 17 JRE image; use a digest reference for repeatable preparation")
    args = parser.parse_args()
    print(json.dumps(prepare(args.runtime_image), indent=2))


if __name__ == "__main__":
    main()
