#!/usr/bin/env python3
"""Convert remaining ASCII diagram blocks in docs to consistent SVG figures.

The source layout is preserved so no information is lost; box-drawing
characters become vector lines, arrows become vector markers, and text is
rendered with a clean sans/mono mix. Run from the repository root:

    python3 website/scripts/vectorize-diagrams.py
"""

from __future__ import annotations

import hashlib
import os
import pathlib
import re

ROOT = pathlib.Path(".")
OUT = ROOT / "website/static/img/diagrams/generated"
CW = 9.4
CH = 19.0
PAD = 34.0
FONT = "ui-monospace, SFMono-Regular, Menlo, monospace"
BOX = set("┌┐└┘├┤┬┴┼╔╗╚╝╠╣╦╩╬─│═║")
ARROWS = set("→←↑↓▼▲◀▶")
SINGLE = set("┌┐└┘├┤┬┴┼─│")
DOUBLE = set("╔╗╚╝╠╣╦╩╬═║")


def esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def segments(char: str, x: float, y: float, w: float, h: float):
    """Return (x1,y1,x2,y2,double) line segments for a box character."""
    cx, cy = x + w / 2, y + h / 2
    left, right, top, bottom = x, x + w, y, y + h
    double = char in DOUBLE
    s = []
    if char in "─═":
        s.append((left, cy, right, cy, double))
    elif char in "│║":
        s.append((cx, top, cx, bottom, double))
    elif char in "┌╔":
        s.append((cx, cy, right, cy, double)); s.append((cx, cy, cx, bottom, double))
    elif char in "┐╗":
        s.append((left, cy, cx, cy, double)); s.append((cx, cy, cx, bottom, double))
    elif char in "└╚":
        s.append((cx, cy, right, cy, double)); s.append((cx, top, cx, cy, double))
    elif char in "┘╝":
        s.append((left, cy, cx, cy, double)); s.append((cx, top, cx, cy, double))
    elif char in "├╠":
        s.append((cx, top, cx, bottom, double)); s.append((cx, cy, right, cy, double))
    elif char in "┤╣":
        s.append((cx, top, cx, bottom, double)); s.append((left, cy, cx, cy, double))
    elif char in "┬╦":
        s.append((left, cy, right, cy, double)); s.append((cx, cy, cx, bottom, double))
    elif char in "┴╩":
        s.append((left, cy, right, cy, double)); s.append((cx, top, cx, cy, double))
    elif char in "┼╬":
        s.append((left, cy, right, cy, double)); s.append((cx, top, cx, bottom, double))
    return s


def arrow_path(char: str, x: float, y: float, w: float, h: float) -> str:
    cx, cy = x + w / 2, y + h / 2
    size = 5.5
    if char in "▼↓":
        return f'<path d="M{cx - size} {cy - size} L{cx + size} {cy - size} L{cx} {cy + size} z" fill="#D97706"/>'
    if char in "▲↑":
        return f'<path d="M{cx - size} {cy + size} L{cx + size} {cy + size} L{cx} {cy - size} z" fill="#D97706"/>'
    if char in "▶→":
        return f'<path d="M{cx - size} {cy - size} L{cx - size} {cy + size} L{cx + size} {cy} z" fill="#D97706"/>'
    if char in "◀←":
        return f'<path d="M{cx + size} {cy - size} L{cx + size} {cy + size} L{cx - size} {cy} z" fill="#D97706"/>'
    return ""


def render(body: str) -> tuple[str, int, int]:
    lines = body.splitlines()
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    width_cells = max((len(line) for line in lines), default=1)
    width = int(PAD * 2 + width_cells * CW)
    height = int(PAD * 2 + len(lines) * CH + 8)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img">',
        f'<rect width="{width}" height="{height}" rx="14" fill="#FFFFFF"/>',
        f'<rect x="0.75" y="0.75" width="{width - 1.5}" height="{height - 1.5}" rx="14" fill="none" stroke="#E2E8F0"/>',
        f'<g font-family="{FONT}" font-size="15">',
    ]
    for row, line in enumerate(lines):
        y = PAD + row * CH
        text_chars = []
        for col, char in enumerate(line):
            x = PAD + col * CW
            if char in BOX:
                for x1, y1, x2, y2, double in segments(char, x, y, CW, CH):
                    color = "#7C3AED" if double else "#A5B4CB"
                    sw = 2.2 if double else 1.3
                    parts.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{color}" stroke-width="{sw}" stroke-linecap="round"/>')
                text_chars.append(" ")
            elif char in ARROWS:
                parts.append(arrow_path(char, x, y, CW, CH))
                text_chars.append(" ")
            else:
                text_chars.append(char)
        text = "".join(text_chars).rstrip()
        if text.strip():
            parts.append(f'<text x="{PAD}" y="{y + CH * 0.72:.1f}" fill="#1F2937" xml:space="preserve">{esc(text)}</text>')
    parts.extend(["</g>", "</svg>", ""])
    return "\n".join(parts), width, height


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    box = re.compile(r"[┌┐└┘│─├┤┬┴┼╔╗╚╝║═╠╣╦╩╬]")
    arrow = re.compile(r"(-->|──>|→|←|↑|↓|▼|▲|◀|▶)")
    replaced = 0
    for path in sorted(ROOT.glob("docs/**/*.md")):
        text = path.read_text(encoding="utf-8")
        output = []
        cursor = 0
        changed = False
        for index, match in enumerate(re.finditer(r"```([^\n]*)\n(.*?)```", text, re.S), start=1):
            body = match.group(2)
            lines = body.splitlines()
            is_box = any(box.search(line) for line in lines)
            arrows = sum(1 for line in lines if arrow.search(line))
            if not (is_box or (arrows >= 3 and len(lines) >= 4)):
                continue
            # Keep pure code/tree listings that contain no box art or arrows.
            digest = hashlib.sha1((str(path) + body).encode("utf-8")).hexdigest()[:10]
            slug = re.sub(r"[^a-z0-9]+", "-", path.stem.lower()).strip("-") or "diagram"
            filename = f"{slug}-{index:02d}-{digest}.svg"
            svg, _, _ = render(body)
            OUT.joinpath(filename).write_text(svg, encoding="utf-8")
            first = next((re.sub(r"[│┌┐└┘─├┤┬┴┼╔╗╚╝║═╠╣╦╩╬]", " ", line).strip() for line in lines if re.sub(r"[│┌┐└┘─├┤┬┴┼╔╗╚╝║═╠╣╦╩╬]", " ", line).strip()), "Diagram")
            alt = re.sub(r"[\[\]]", "", first)[:110]
            output.append(text[cursor:match.start()])
            image_path = pathlib.Path(os.path.relpath(OUT / filename, path.parent)).as_posix()
            output.append(f"![{alt}]({image_path})")
            cursor = match.end()
            changed = True
            replaced += 1
        output.append(text[cursor:])
        if changed:
            path.write_text("".join(output), encoding="utf-8")
    print(f"vectorized {replaced} diagram blocks into {OUT}")


if __name__ == "__main__":
    main()
