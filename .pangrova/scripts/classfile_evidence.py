"""Conservative evidence that a freshly compiled source has no JVM Code attributes."""
from functools import lru_cache
from pathlib import Path
import struct


def class_evidence(path):
    data = path.read_bytes()
    offset = 0

    def take(size):
        nonlocal offset
        if size < 0 or offset + size > len(data):
            raise ValueError(f"Truncated class file: {path}")
        result = data[offset:offset + size]
        offset += size
        return result

    def number(size):
        return int.from_bytes(take(size), "big")

    if number(4) != 0xCAFEBABE:
        raise ValueError(f"Invalid class file: {path}")
    take(4)  # minor and major version
    count = number(2)
    strings = {}
    index = 1
    while index < count:
        tag = number(1)
        if tag == 1:
            strings[index] = take(number(2))
        elif tag in (5, 6):
            take(8)
            index += 1
        elif tag in (3, 4, 9, 10, 11, 12, 17, 18):
            take(4)
        elif tag in (7, 8, 16, 19, 20):
            take(2)
        elif tag == 15:
            take(3)
        else:
            raise ValueError(f"Unknown class constant tag {tag}: {path}")
        index += 1

    def attributes():
        result = []
        for _ in range(number(2)):
            key = strings.get(number(2))
            payload = take(number(4))
            if key is None:
                raise ValueError(f"Invalid class attribute name: {path}")
            result.append((key, payload))
        return result

    take(6)  # access, this class, super class
    take(2 * number(2))  # interfaces
    has_code = False
    for section in range(2):  # fields, then methods
        for _ in range(number(2)):
            take(6)  # access, name, descriptor
            attrs = attributes()
            if section == 1:
                has_code |= any(key == b"Code" for key, _ in attrs)
    source = None
    for key, payload in attributes():
        if key == b"SourceFile":
            if len(payload) != 2:
                raise ValueError(f"Invalid SourceFile attribute: {path}")
            source = strings.get(struct.unpack(">H", payload)[0])
    if offset != len(data):
        raise ValueError(f"Trailing class bytes: {path}")
    return source, has_code


@lru_cache(maxsize=None)
def package_evidence(directory):
    return [(path, path.stat().st_mtime, *class_evidence(path))
            for path in Path(directory).glob("*.class")]


def has_no_executable_code(root, name):
    module, source = name.split("/src/main/java/", 1)
    package, filename = source.rsplit("/", 1)
    source_time = (root / name).stat().st_mtime
    files = [item for item in package_evidence(str(root / module / "target/classes" / package))
             if item[2] == filename.encode("utf-8")]
    return bool(files) and all(time >= source_time and not code for _, time, _, code in files)
