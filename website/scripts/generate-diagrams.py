#!/usr/bin/env python3
"""Generate the documentation diagram set as static SVG files.

The diagrams share one visual system: light cards, violet control-plane accents,
amber data-plane accents, and protocol chips. Run from the repository root:

    python3 website/scripts/generate-diagrams.py
"""

from __future__ import annotations

import pathlib
import textwrap

OUT = pathlib.Path("website/static/img/diagrams")
W = 1200

FONT = "ui-sans-serif, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, monospace"


def esc(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def svg_open(height: int, title: str, desc: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{height}" '
        f'viewBox="0 0 {W} {height}" role="img" aria-labelledby="t d">',
        f'<title id="t">{esc(title)}</title>',
        f'<desc id="d">{esc(desc)}</desc>',
        "<defs>",
        '<linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">'
        '<stop offset="0%" stop-color="#F7F5FF"/>'
        '<stop offset="55%" stop-color="#F8FAFC"/>'
        '<stop offset="100%" stop-color="#F1F7FB"/></linearGradient>',
        '<filter id="shadow" x="-10%" y="-10%" width="120%" height="130%">'
        '<feDropShadow dx="0" dy="6" stdDeviation="9" flood-color="#1E1B4B" flood-opacity="0.10"/>'
        "</filter>",
        '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
        '<path d="M0 0 L10 5 L0 10 z" fill="#7C3AED"/></marker>',
        '<marker id="arrowAmber" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
        '<path d="M0 0 L10 5 L0 10 z" fill="#D97706"/></marker>',
        '<marker id="arrowGreen" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
        '<path d="M0 0 L10 5 L0 10 z" fill="#34D399"/></marker>',
        "</defs>",
        f'<rect width="{W}" height="{height}" fill="url(#bg)"/>',
        '<g font-family="' + FONT + '">',
    ]


def svg_close() -> list[str]:
    return ["</g>", "</svg>", ""]


def title_block(lines: list[str], title: str, subtitle: str) -> None:
    lines.append(
        f'<text x="60" y="74" font-size="34" font-weight="800" fill="#111827">{esc(title)}</text>'
    )
    lines.append(
        f'<text x="60" y="106" font-size="17" fill="#64748B">{esc(subtitle)}</text>'
    )


def card(
    lines: list[str],
    x: int,
    y: int,
    w: int,
    h: int,
    heading: str,
    body: list[str],
    accent: str = "#7C3AED",
    fill: str = "#FFFFFF",
    chip: str | None = None,
) -> None:
    lines.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{fill}" '
        f'stroke="#E2E8F0" filter="url(#shadow)"/>'
    )
    lines.append(
        f'<rect x="{x}" y="{y}" width="7" height="{h}" rx="3.5" fill="{accent}"/>'
    )
    lines.append(
        f'<text x="{x + 30}" y="{y + 36}" font-size="24" font-weight="700" fill="#111827">{esc(heading)}</text>'
    )
    if chip:
        cw = 30 + len(chip) * 8.2
        lines.append(
            f'<rect x="{x + w - cw - 20}" y="{y + 14}" width="{cw}" height="30" rx="15" '
            f'fill="#F5F3FF" stroke="#DDD6FE"/>'
        )
        lines.append(
            f'<text x="{x + w - cw / 2 - 20}" y="{y + 33}" font-size="14" font-weight="700" fill="{accent}" text-anchor="middle">{esc(chip)}</text>'
        )
    ty = y + 66
    for entry in body:
        if entry.startswith("|"):
            # chip row: |chip|chip|
            labels = [p for p in entry.split("|") if p.strip()]
            cx = x + 30
            for label in labels:
                cw = 28 + len(label) * 8.2
                lines.append(
                    f'<rect x="{cx}" y="{ty - 18}" width="{cw}" height="28" rx="14" '
                    f'fill="#F8FAFC" stroke="#E2E8F0"/>'
                )
                lines.append(
                    f'<text x="{cx + cw / 2}" y="{ty}" font-size="14" font-weight="600" fill="#475569" text-anchor="middle">{esc(label)}</text>'
                )
                cx += cw + 10
            ty += 36
        else:
            lines.append(
                f'<text x="{x + 30}" y="{ty}" font-size="16.5" fill="#475569">{esc(entry)}</text>'
            )
            ty += 29


def arrow(lines: list[str], x1: int, y1: int, x2: int, y2: int, label: str, color: str = "#7C3AED", dashed: bool = False) -> None:
    marker = "arrowAmber" if color == "#D97706" else ("arrowGreen" if color == "#34D399" else "arrow")
    dash = ' stroke-dasharray="8 6"' if dashed else ""
    lines.append(
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="2.4"{dash} marker-end="url(#{marker})"/>'
    )
    if label:
        lines.append(
            f'<text x="{(x1 + x2) / 2 + 10}" y="{(y1 + y2) / 2 + 4}" font-size="14" font-weight="600" fill="{color}">{esc(label)}</text>'
        )


def legend(lines: list[str], y: int) -> None:
    lines.append(
        f'<line x1="60" y1="{y}" x2="100" y2="{y}" stroke="#7C3AED" stroke-width="2.4"/>'
    )
    lines.append(f'<text x="110" y="{y + 5}" font-size="12.5" fill="#64748B">control plane</text>')
    lines.append(
        f'<line x1="240" y1="{y}" x2="280" y2="{y}" stroke="#D97706" stroke-width="2.4" stroke-dasharray="8 6"/>'
    )
    lines.append(f'<text x="290" y="{y + 5}" font-size="12.5" fill="#64748B">data plane</text>')


def blog_execution_path() -> None:
    h = 1080
    lines = svg_open(h, "Query execution path", "From SQL text through Coordinator planning to Native execution and Arrow Flight results.")
    title_block(
        lines,
        "Query execution path",
        "Control decisions stay in Java; relational work and data movement stay Native.",
    )
    x, w = 100, 1000
    card(lines, x, 140, w, 100, "Clients", ["JDBC · REST API · CLI", "SQL text · session context · result consumer"], "#2563EB", chip="HTTP")
    arrow(lines, 600, 240, 600, 276, "submit / status / cancel")
    card(
        lines,
        x,
        280,
        w,
        150,
        "Coordinator JVM",
        [
            "|parse|bind|authorize|plan|admit|schedule|fence|commit|",
            "Resolves catalog metadata and snapshots, chooses Workers, and owns the final fenced catalog commit.",
        ],
        "#7C3AED",
        chip="Java",
    )
    arrow(lines, 600, 430, 600, 466, "canonical v2 task request")
    card(
        lines,
        x,
        470,
        w,
        120,
        "Canonical task binding",
        [
            "TaskRequest v2 + Substrait plan + execution bindings",
            "|descriptor hash|snapshot|authority|lease|write intent|",
        ],
        "#7C3AED",
    )
    arrow(lines, 600, 590, 600, 626, "JNI")
    card(
        lines,
        x,
        630,
        w,
        110,
        "Worker JVM",
        ["lifecycle · admission · JNI · secrets · governed provider callbacks"],
        "#0D9488",
        chip="Java",
    )
    arrow(lines, 600, 740, 600, 776, "JNI")
    card(
        lines,
        x,
        780,
        w,
        130,
        "C++ Native — Velox",
        [
            "scan → filter/project → join/aggregate → retrieve → shuffle → fill → output",
            "Vectorized operators consume Arrow batches and report status through the canonical lifecycle.",
        ],
        "#D97706",
        chip="C++20",
    )
    arrow(lines, 600, 910, 600, 946, "Arrow Flight results", "#D97706", dashed=True)
    card(lines, x, 950, w, 70, "Client result stream", ["Output Worker streams batches directly to the consumer."], "#D97706")
    legend(lines, 1050)
    lines.extend(svg_close())
    OUT.joinpath("blog-query-execution.svg").write_text("\n".join(lines), encoding="utf-8")


def panel(
    lines: list[str],
    x: int,
    y: int,
    w: int,
    h: int,
    heading: str,
    accent: str = "#7C3AED",
    fill: str = "#FFFFFF",
    dashed: bool = False,
) -> None:
    dash = ' stroke-dasharray="9 7"' if dashed else ""
    lines.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="18" fill="{fill}" '
        f'stroke="{accent}" stroke-opacity="0.45" stroke-width="1.6"{dash}/>'
    )
    lines.append(
        f'<text x="{x + 24}" y="{y + 32}" font-size="19" font-weight="700" fill="{accent}">{esc(heading)}</text>'
    )


def node(
    lines: list[str],
    x: int,
    y: int,
    w: int,
    h: int,
    heading: str,
    sub: list[str],
    accent: str = "#7C3AED",
) -> None:
    lines.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="13" fill="#FFFFFF" '
        f'stroke="#E2E8F0" filter="url(#shadow)"/>'
    )
    lines.append(f'<rect x="{x}" y="{y}" width="5" height="{h}" rx="2.5" fill="{accent}"/>')
    lines.append(
        f'<text x="{x + 20}" y="{y + 28}" font-size="16.5" font-weight="700" fill="#111827">{esc(heading)}</text>'
    )
    ty = y + 52
    for entry in sub:
        lines.append(
            f'<text x="{x + 20}" y="{ty}" font-size="13.5" fill="#64748B">{esc(entry)}</text>'
        )
        ty += 21


def planning_stack() -> None:
    h = 860
    lines = svg_open(h, "Planning and execution stack", "SQL is parsed, planned with Calcite, serialized as Substrait, and executed by Velox.")
    title_block(lines, "Planning and execution stack", "One canonical plan crosses the Java/Native boundary.")
    x, w = 180, 840
    steps = [
        ("SQL and API", "JDBC · REST API · CLI", "#2563EB"),
        ("ANTLR parser", "Custom SQL extensions · statement classification · partial diagnostics", "#7C3AED"),
        ("Calcite optimizer", "Rule- and cost-based transformations · statistics · type validation", "#7C3AED"),
        ("Substrait plan", "Canonical cross-language plan · explicit version stamping", "#0D9488"),
        ("Velox engine", "C++20 vectorized operators · Arrow batches · Native result stream", "#D97706"),
    ]
    y = 150
    for i, (heading, sub, color) in enumerate(steps):
        card(lines, x, y, w, 92, heading, [sub], color)
        if i < len(steps) - 1:
            arrow(lines, x + w / 2, y + 92, x + w / 2, y + 132, "")
        y += 132
    legend(lines, 820)
    lines.extend(svg_close())
    OUT.joinpath("planning-stack.svg").write_text("\n".join(lines), encoding="utf-8")


def distributed_topology() -> None:
    h = 940
    lines = svg_open(h, "Coordinator high availability topology", "Load balancer, coordinator lease, workers, PostgreSQL, and object storage.")
    title_block(lines, "Coordinator high availability", "Leases and fencing make one Coordinator authoritative; workers and storage remain external dependencies.")
    panel(lines, 60, 140, 1080, 760, "", "#7C3AED", "#FBFBFE")
    node(lines, 440, 165, 320, 78, "Load balancer", ["HAProxy · ALB · SLB · HTTPS :8080"], "#2563EB")
    arrow(lines, 560, 243, 430, 285, ":8080")
    arrow(lines, 640, 243, 770, 285, "")
    node(lines, 230, 285, 300, 92, "Coordinator — leader", ["Serves queries and DDL", "Owns leases and final commit"], "#7C3AED")
    node(lines, 670, 285, 300, 92, "Coordinator — follower", ["Registers for failover", "No concurrent writes"], "#7C3AED")
    arrow(lines, 380, 377, 500, 440, "lease renewal", "#7C3AED")
    arrow(lines, 820, 377, 700, 440, "lease renewal", "#7C3AED")
    node(lines, 400, 440, 400, 84, "PostgreSQL", ["HA lease · metadata · journal · quotas"], "#0D9488")
    for i, label in enumerate(["Worker-0", "Worker-1", "Worker-2"]):
        x = 170 + i * 320
        node(lines, x, 610, 260, 84, label, ["JVM + Native execution", "Arrow Flight exchange"], "#D97706")
        arrow(lines, x + 130, 694, 600, 790, "", "#D97706", dashed=True)
    lines.append('<polyline points="230,331 150,331 150,580 300,580 300,610" fill="none" stroke="#7C3AED" stroke-width="2.4" marker-end="url(#arrow)"/>')
    lines.append('<polyline points="150,580 620,580 620,610" fill="none" stroke="#7C3AED" stroke-width="2.4" marker-end="url(#arrow)"/>')
    lines.append('<polyline points="150,580 940,580 940,610" fill="none" stroke="#7C3AED" stroke-width="2.4" marker-end="url(#arrow)"/>')
    lines.append('<text x="620" y="568" font-size="13.5" font-weight="600" fill="#7C3AED" text-anchor="middle">task control</text>')
    node(lines, 350, 790, 500, 84, "Object storage", ["S3 · OSS · MinIO · HDFS — lakehouse data files"], "#0D9488")
    legend(lines, 910)
    lines.extend(svg_close())
    OUT.joinpath("distributed-topology.svg").write_text("\n".join(lines), encoding="utf-8")


def connectors_plugin() -> None:
    h = 900
    lines = svg_open(h, "Connector plugin architecture", "Versioned factories are discovered per role, isolated by classloader, and exposed through the Connector SPI.")
    title_block(lines, "Connector plugin architecture", "Factories and providers are isolated; the engine compiles against contracts only.")
    panel(lines, 60, 140, 1080, 230, "INVENTORY", "#7C3AED", "#FBFBFE")
    node(lines, 90, 190, 310, 120, "Factory discovery", ["ServiceLoader per role", "Duplicate names and versions rejected"], "#2563EB")
    node(lines, 430, 190, 310, 120, "Classloader isolation", ["Builtin factories on the role classpath", "Each external directory gets one loader"], "#0D9488")
    node(lines, 770, 190, 340, 120, "Lifecycle", ["Restart to update plugins", "Active leases block loader reclamation"], "#D97706")
    arrow(lines, 600, 370, 600, 410, "connector contracts", "#7C3AED")
    panel(lines, 60, 410, 1080, 210, "CONNECTOR SPI", "#7C3AED")
    spi = ["Metadata", "Split planning", "Page source", "Page sink", "Transaction", "Index & statistics"]
    for i, label in enumerate(spi):
        x = 90 + (i % 3) * 350
        y = 465 + (i // 3) * 64
        lines.append(f'<rect x="{x}" y="{y}" width="320" height="46" rx="10" fill="#F5F3FF" stroke="#DDD6FE"/>')
        lines.append(f'<text x="{x + 160}" y="{y + 29}" font-size="14.5" font-weight="600" fill="#5B21B6" text-anchor="middle">{esc(label)}</text>')
    arrow(lines, 600, 620, 600, 660, "bindings", "#7C3AED")
    for i, (label, sub) in enumerate([
        ("Apache Iceberg", ["Native catalog · snapshots · metadata tables"]),
        ("Apache Paimon", ["Filesystem or REST catalog · merge policies"]),
        ("Managed Lance", ["Native dataset control · snapshot isolation"]),
    ]):
        x = 90 + i * 360
        arrow(lines, 600, 660, x + 160, 690, "", "#7C3AED")
        node(lines, x, 690, 320, 92, label, sub, "#0D9488")
    lines.append(f'<text x="600" y="830" font-size="14" fill="#64748B" text-anchor="middle">Additional formats register through the same factory contract; updates require a restart.</text>')
    legend(lines, 870)
    lines.extend(svg_close())
    OUT.joinpath("connectors-plugin.svg").write_text("\n".join(lines), encoding="utf-8")


def native_layers() -> None:
    h = 900
    lines = svg_open(h, "Native execution layers", "Java control plane, connector contracts, Protobuf task bindings, and C++/Velox execution.")
    title_block(lines, "Native execution layers", "Control-plane decisions are serialized once; Native execution owns relational work.")
    panel(lines, 60, 140, 1080, 250, "CONTROL PLANE — JAVA", "#7C3AED", "#FBFBFE")
    names = [("SQL parser", "ANTLR"), ("Calcite planner", "rules · cost"), ("Substrait", "canonical plan"), ("Split planning", "connector bindings")]
    for i, (name, sub) in enumerate(names):
        node(lines, 90 + i * 260, 195, 230, 86, name, [sub], "#7C3AED")
        if i < 3:
            arrow(lines, 320 + i * 260, 238, 350 + i * 260, 238, "", "#7C3AED")
    node(lines, 90, 305, 1020, 62, "Connector SPI — metadata, splits, page sinks, statistics", [], "#0D9488")
    arrow(lines, 600, 390, 600, 440, "TaskRequest v2 + Substrait + bindings", "#7C3AED")
    panel(lines, 60, 440, 1080, 360, "DATA PLANE — C++ / VELOX", "#D97706", "#FFFCF5")
    node(lines, 90, 495, 480, 120, "Native connector SPI", ["DataSource factory", "DataSource reader", "Native writer · spiller"], "#0D9488")
    node(lines, 650, 495, 460, 120, "Vectorized operators", ["scan · filter · project", "join · aggregate · sort", "retrieve · shuffle · fill · output"], "#D97706")
    arrow(lines, 550, 555, 650, 555, "", "#D97706", dashed=True)
    lines.append('<text x="600" y="543" font-size="11.5" font-weight="600" fill="#D97706" text-anchor="middle">Arrow batches</text>')
    node(lines, 90, 645, 1020, 90, "Result and exchange", ["Native Flight shuffle · Arrow Flight result stream", "Canonical lifecycle and error reports return to the Worker JVM"], "#D97706")
    arrow(lines, 600, 800, 600, 840, "JNI status / terminal report", "#7C3AED")
    legend(lines, 890)
    lines.extend(svg_close())
    OUT.joinpath("native-layers.svg").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    blog_execution_path()
    planning_stack()
    distributed_topology()
    connectors_plugin()
    native_layers()
    query_execution_flow()
    metadata_layers()
    security_flow()
    streaming_pipeline()
    caching_layers()
    observability_pipeline()
    testing_levels()
    resilience_flow()
    memory_arbitration()
    tenant_isolation()
    governance_pipeline()
    optimization_surfaces()
    helm_topology()
    jni_ownership()
    refresh_flow()
    print("generated diagrams in", OUT)




def query_execution_flow() -> None:
    h = 1260
    lines = svg_open(h, "End-to-end query execution", "Client submission, Coordinator planning, Native execution, and result delivery.")
    title_block(lines, "End-to-end query execution", "The Coordinator plans and fences; Workers execute; one Output Worker streams results.")
    panel(lines, 60, 140, 1080, 260, "COORDINATOR — JAVA", "#7C3AED", "#FBFBFE")
    tops = [
        ("Parse · bind", "ANTLR · session resolution", "authorize"),
        ("Plan", "Calcite · statistics", "Substrait"),
        ("Admit · schedule", "resource and tenant limits", "stage DAG"),
    ]
    for i, (heading, sub, extra) in enumerate(tops):
        node(lines, 90 + i * 350, 195, 320, 92, heading, [sub, extra], "#7C3AED")
        if i < 2:
            arrow(lines, 410 + i * 350, 241, 440 + i * 350, 241, "", "#7C3AED")
    node(lines, 90, 305, 1020, 64, "Query state · admission · worker placement · leases · final catalog commit", [], "#0D9488")
    arrow(lines, 600, 400, 600, 450, "TaskRequest v2 + Substrait + execution bindings", "#7C3AED")
    panel(lines, 60, 450, 1080, 400, "WORKER — JAVA + NATIVE", "#D97706", "#FFFCF5")
    node(lines, 90, 505, 470, 120, "Worker JVM", ["task lifecycle · admission", "JNI bridge · connector callbacks", "status and terminal reports"], "#0D9488")
    node(lines, 650, 505, 460, 120, "Native Worker — C++ / Velox", ["scan · filter · project", "join · aggregate · shuffle", "retrieval · write · output"], "#D97706")
    arrow(lines, 560, 565, 650, 565, "", "#D97706", dashed=True)
    lines.append('<text x="605" y="553" font-size="11.5" font-weight="600" fill="#D97706" text-anchor="middle">JNI</text>')
    node(lines, 90, 660, 1020, 130, "Execution and exchange", ["Workers exchange Arrow Flight batches inside a stage", "The Output Worker streams the result to the client over Arrow Flight", "Write attempts return commit fragments to the Coordinator"], "#D97706")
    arrow(lines, 600, 850, 600, 900, "")
    card(lines, 180, 900, 840, 84, "Client result stream", ["Arrow Flight batches from the Output Worker; cancellation and status use HTTP"], "#D97706")
    lines.append('<polyline points="1140,330 1180,330 1180,1052 1020,1052" fill="none" stroke="#7C3AED" stroke-width="2.4" marker-end="url(#arrow)"/>')
    lines.append('<text x="1170" y="700" font-size="13" font-weight="600" fill="#7C3AED" text-anchor="middle" transform="rotate(-90 1170 700)">status / terminal</text>')
    card(lines, 180, 1010, 840, 84, "Coordinator terminal handling", ["Validates terminal reports and publishes the final fenced catalog commit for writes"], "#7C3AED")
    legend(lines, 1215)
    lines.extend(svg_close())
    OUT.joinpath("query-execution-flow.svg").write_text("\n".join(lines), encoding="utf-8")


def metadata_layers() -> None:
    h = 980
    lines = svg_open(h, "Metadata architecture", "Catalog deployments, federation, tiered cache, projections, and durable persistence.")
    title_block(lines, "Metadata architecture", "Catalog identity and projections are fenced to one deployment and revision.")
    node(lines, 80, 140, 1040, 80, "Callers", ["Parser and planner metadata resolution · Connector catalog handles · DDL and maintenance tasks"], "#2563EB")
    arrow(lines, 600, 220, 600, 258, "", "#7C3AED")
    panel(lines, 60, 258, 1080, 220, "CATALOG LAYER", "#7C3AED", "#FBFBFE")
    node(lines, 90, 310, 330, 130, "Catalog registry", ["Deployment identity", "Catalog revisions", "Provider selection"], "#7C3AED")
    node(lines, 435, 310, 330, 130, "Federated catalogs", ["External catalog handles", "Namespaces, tables, columns", "Tenant-scoped access"], "#7C3AED")
    node(lines, 780, 310, 330, 130, "Metadata cache", ["L1 Caffeine · short TTL", "L2 memory or Redis", "Generation-checked fill"], "#0D9488")
    arrow(lines, 600, 478, 600, 518, "", "#7C3AED")
    panel(lines, 60, 518, 1080, 190, "PROJECTIONS", "#0D9488")
    node(lines, 90, 570, 490, 100, "Table and schema projections", ["Rebuildable read models", "Deployment and revision fenced", "Invalidation events"], "#0D9488")
    node(lines, 620, 570, 490, 100, "Synchronization", ["Source progress and diagnostics", "Atomic completion receipts", "Observation epochs"], "#0D9488")
    arrow(lines, 600, 708, 600, 748, "", "#7C3AED")
    node(lines, 80, 748, 1040, 90, "Catalog persistence — PostgreSQL", ["Deployment history · projections · sync state · outbox · authorization-gate receipts"], "#0D9488")
    lines.append('<text x="600" y="890" font-size="14" fill="#64748B" text-anchor="middle">Projection writes, outbox events, and receipts commit in one transaction; reads may be cached but never bypass the current deployment fence.</text>')
    legend(lines, 950)
    lines.extend(svg_close())
    OUT.joinpath("metadata-layers.svg").write_text("\n".join(lines), encoding="utf-8")


def security_flow() -> None:
    h = 1000
    lines = svg_open(h, "Security decision flow", "Authentication, authorization, row and column policies, execution binding, and audit.")
    title_block(lines, "Security decision flow", "Every request is authenticated, authorized against current state, and audited.")
    x, w = 120, 960
    steps = [
        ("Request", "HTTP · JDBC · CLI · internal control channel", "#2563EB"),
        ("Authentication", "Internal token · LDAP · OIDC / OAuth2 · explicit NOOP for development", "#7C3AED"),
        ("Authorization", "Current roles and privileges · exact ACTIVE tenant · catalog and table grants", "#7C3AED"),
        ("Policy evaluation", "Row filters · column masks · quality rules · model and capability allowlists", "#0D9488"),
        ("Execution binding", "Authority receipt · lease identity · task and write admission", "#7C3AED"),
        ("Audit and lineage", "Security events · query audit records · lineage publication", "#D97706"),
    ]
    y = 140
    for i, (heading, sub, color) in enumerate(steps):
        card(lines, x, y, w, 84, heading, [sub], color)
        if i < len(steps) - 1:
            arrow(lines, x + w / 2, y + 84, x + w / 2, y + 118, "")
        y += 118
    arrow(lines, 120, y - 30, 1080, y - 30, "", "#94A3B8", dashed=True)
    card(lines, 120, y + 30, 960, 84, "Privilege administration", ["GRANT / REVOKE through PostgreSQL: current executor check, lock order, durable receipt, and SHOW PRIVILEGE MUTATION"], "#0D9488")
    legend(lines, 970)
    lines.extend(svg_close())
    OUT.joinpath("security-flow.svg").write_text("\n".join(lines), encoding="utf-8")


def streaming_pipeline() -> None:
    h = 980
    lines = svg_open(h, "Streaming and CDC pipeline", "Source ingestion through CDC decoding, two-phase commit, connector writes, and checkpoint state.")
    title_block(lines, "Streaming and CDC pipeline", "CDC ingestion uses a Coordinator-local writer today; offsets and catalog publication are separate commits.")
    node(lines, 80, 140, 1040, 76, "Source", ["Kafka topics · partitions · consumer groups"], "#2563EB")
    arrow(lines, 600, 216, 600, 254, "", "#34D399")
    node(lines, 80, 254, 1040, 76, "CdcSourceReader", ["Source-specific SDK, seek, rebalance, and commit"], "#34D399")
    arrow(lines, 600, 330, 600, 368, "", "#34D399")
    panel(lines, 60, 368, 1080, 190, "DECODE AND BUFFER", "#34D399", "#F6FEFB")
    node(lines, 90, 420, 320, 110, "Format decoder", ["Canal · Debezium · Maxwell", "Typed CDC envelopes"], "#34D399")
    node(lines, 440, 420, 320, 110, "CdcPipelineLoop", ["Mini-batch buffering", "Watermark and late data"], "#34D399")
    node(lines, 790, 420, 310, 110, "Checkpoint store", ["Offset and watermark state", "Recovery seek"], "#0D9488")
    arrow(lines, 600, 558, 600, 598, "", "#D97706")
    panel(lines, 60, 598, 1080, 190, "COMMIT PATH", "#D97706", "#FFFCF5")
    node(lines, 90, 650, 320, 110, "CdcTwoPhaseCommitter", ["Prepare and commit", "Abort on failure"], "#D97706")
    node(lines, 440, 650, 320, 110, "ConnectorPageSinkCdcWriter", ["Connector SDK page sink", "Coordinator-local today"], "#D97706")
    node(lines, 790, 650, 310, 110, "Catalog publication", ["Iceberg / Paimon commit", "Final fenced publication"], "#0D9488")
    lines.append('<text x="600" y="840" font-size="14" fill="#64748B" text-anchor="middle">Source offsets, checkpoints, and catalog publication are separate commits. Exactly-once/effectively-once delivery is not established.</text>')
    legend(lines, 930)
    lines.extend(svg_close())
    OUT.joinpath("streaming-pipeline.svg").write_text("\n".join(lines), encoding="utf-8")



def caching_layers() -> None:
    h = 930
    lines = svg_open(h, "Cache architecture", "Metadata caches and Native caching, with the proposed unified RAM/SSD manager.")
    title_block(lines, "Cache architecture", "Implemented caches are separated from the proposed unified multi-tier manager.")
    node(lines, 80, 140, 1040, 76, "Callers", ["Planner and metadata resolution · Worker scans · Connector file access"], "#2563EB")
    panel(lines, 60, 250, 660, 230, "IMPLEMENTED", "#7C3AED", "#FBFBFE")
    node(lines, 90, 300, 290, 150, "Metadata L1", ["Caffeine · on-heap", "10k entries · 60s TTL", "Per-process"], "#7C3AED")
    node(lines, 400, 300, 290, 150, "Metadata L2", ["Memory or Redis", "Generation-checked fill", "Invalidation by group and entry"], "#0D9488")
    arrow(lines, 380, 375, 400, 375, "", "#7C3AED", dashed=True)
    panel(lines, 740, 250, 400, 230, "NATIVE CACHE", "#D97706", "#FFFCF5")
    node(lines, 770, 300, 340, 150, "Velox async cache", ["RAM budget · optional SSD path", "Shards and checkpoint interval", "Worker-local eviction"], "#D97706")
    arrow(lines, 600, 216, 520, 250, "", "#7C3AED")
    arrow(lines, 600, 216, 940, 250, "", "#D97706")
    arrow(lines, 600, 480, 600, 530, "", "#7C3AED")
    panel(lines, 60, 530, 1080, 210, "TARGET DESIGN", "#94A3B8", "#FFFFFF", dashed=True)
    node(lines, 90, 585, 330, 120, "Unified RAM/SSD manager", ["Per-table cache accounting", "Eviction policy interface"], "#94A3B8")
    node(lines, 435, 585, 330, 120, "Adaptive prefetch", ["Column and block prediction", "Evidence required before enabling"], "#94A3B8")
    node(lines, 780, 585, 330, 120, "Remote tiers", ["Object store and remote cache", "Latency and cost accounting"], "#94A3B8")
    arrow(lines, 600, 480, 600, 530, "", "#94A3B8", dashed=True)
    node(lines, 280, 790, 640, 76, "Remote object storage", ["S3 · OSS · HDFS — the source of truth"], "#0D9488")
    arrow(lines, 600, 740, 600, 790, "", "#94A3B8", dashed=True)
    legend(lines, 900)
    lines.extend(svg_close())
    OUT.joinpath("caching-layers.svg").write_text("\n".join(lines), encoding="utf-8")


def observability_pipeline() -> None:
    h = 980
    lines = svg_open(h, "Observability pipeline", "Instrumented components, SDK providers, sampling, and OTLP export.")
    title_block(lines, "Observability pipeline", "Providers are configured per role; disabled deployments use the local NoOp implementation.")
    node(lines, 80, 140, 1040, 80, "Instrumented components", ["Coordinator · Worker JVM · connector callbacks · Native execution"], "#2563EB")
    arrow(lines, 600, 220, 600, 260, "", "#7C3AED")
    panel(lines, 60, 260, 1080, 200, "SDK AND PROVIDERS", "#7C3AED", "#FBFBFE")
    node(lines, 90, 315, 250, 120, "Metrics", ["Counters and gauges", "Metric attributes and units"], "#7C3AED")
    node(lines, 360, 315, 250, 120, "Tracing", ["Spans and current context", "Sampling policy"], "#7C3AED")
    node(lines, 630, 315, 250, 120, "Logging", ["Structured event keys", "MDC enrichment"], "#7C3AED")
    node(lines, 900, 315, 210, 120, "Profiling", ["Delegated Native profiling", "Unavailable profiles rejected"], "#7C3AED")
    arrow(lines, 600, 460, 600, 500, "", "#7C3AED")
    panel(lines, 60, 500, 1080, 210, "EXPORT", "#0D9488", "#F7FEFC")
    node(lines, 90, 555, 330, 120, "OTLP exporter", ["gRPC endpoint", "Compression and TLS", "Configured headers"], "#0D9488")
    node(lines, 450, 555, 300, 120, "Sampling", ["Probability or rate limiting", "Root trace decisions"], "#0D9488")
    node(lines, 780, 555, 330, 120, "NoOp provider", ["Explicitly disabled exporters", "No silent fallback"], "#94A3B8")
    arrow(lines, 600, 710, 600, 750, "", "#0D9488")
    node(lines, 280, 750, 640, 80, "Collector and backends", ["OTel collector · Prometheus · tracing backend · log store"], "#0D9488")
    lines.append('<text x="600" y="880" font-size="14" fill="#64748B" text-anchor="middle">Telemetry is diagnostic evidence, not durable audit. Local query events do not replace the audit store.</text>')
    legend(lines, 950)
    lines.extend(svg_close())
    OUT.joinpath("observability-pipeline.svg").write_text("\n".join(lines), encoding="utf-8")


def testing_levels() -> None:
    h = 900
    lines = svg_open(h, "Testing levels", "Real dependencies at every level, with no mocks or test doubles.")
    title_block(lines, "Testing levels", "All levels use real dependencies; mocks, stubs, and test doubles are prohibited.")
    levels = [
        ("Benchmark and chaos", "JMH · TPC-derived suites · fault injection", "#D97706", 420),
        ("Native and sanitizers", "GoogleTest · CTest · ASan/UBSan · packaged JNI", "#7C3AED", 540),
        ("End-to-end", "Testcontainers · real Coordinator/Worker · real model and storage fixtures", "#0D9488", 660),
        ("Module integration", "Owning module dependencies · real PostgreSQL, Kafka, MinIO, Redis", "#2563EB", 780),
    ]
    y = 140
    for heading, sub, color, w in levels:
        x = (1200 - w) / 2
        card(lines, int(x), y, w, 78, heading, [sub], color)
        y += 120
    card(lines, 180, 620, 840, 112, "Unit", ["Owning module · real lightweight dependencies", "Every commit"], "#7C3AED")
    lines.append('<text x="600" y="800" font-size="14" fill="#64748B" text-anchor="middle">Coverage depth follows risk: pure refactoring may rely on existing tests; new behavior needs boundary and error-path coverage.</text>')
    lines.append('<text x="600" y="826" font-size="14" fill="#64748B" text-anchor="middle">Container and Native suites require their real prerequisites; unavailable prerequisites are reported, never silently skipped.</text>')
    lines.extend(svg_close())
    OUT.joinpath("testing-levels.svg").write_text("\n".join(lines), encoding="utf-8")



def simple_flow(filename: str, title: str, subtitle: str, steps, note: str | None = None) -> None:
    h = 140 + len(steps) * 118 + 80
    lines = svg_open(h, title, subtitle)
    title_block(lines, title, subtitle)
    x, w = 120, 960
    y = 140
    for i, (heading, sub, color) in enumerate(steps):
        card(lines, x, y, w, 84, heading, [sub], color)
        if i < len(steps) - 1:
            arrow(lines, x + w / 2, y + 84, x + w / 2, y + 118, "")
        y += 118
    if note:
        lines.append(f'<text x="600" y="{y + 20}" font-size="14" fill="#64748B" text-anchor="middle">{esc(note)}</text>')
    legend(lines, h - 24)
    lines.extend(svg_close())
    OUT.joinpath(filename).write_text("\n".join(lines), encoding="utf-8")


def resilience_flow() -> None:
    simple_flow("resilience-flow.svg",
        "Failure handling and recovery",
        "Classify the failure, apply a bounded response, and surface the exact client outcome.",
        [
            ("Failure", "Task error · Worker loss · Coordinator loss · storage or network interruption", "#2563EB"),
            ("Classify", "Error category · retryability · attempt and generation ownership", "#7C3AED"),
            ("Contain", "Circuit breaker · bounded retry budget · cancellation · drain", "#D97706"),
            ("Recover or degrade", "Retry dispatch · stage rebuild · fenced takeover · explicit unsupported result", "#0D9488"),
            ("Report and audit", "Client error mapping · terminal state · audit and lineage records", "#7C3AED"),
        ],
        "Static analysis and retries cannot turn an unverified recovery path into a qualified guarantee.")


def memory_arbitration() -> None:
    h = 980
    lines = svg_open(h, "Memory arbitration", "Node pools, task budgets, shuffle and cache reservations, and spilling.")
    title_block(lines, "Memory arbitration", "Every allocation is admitted against the process budget; pressure triggers spill or rejection.")
    node(lines, 80, 140, 1040, 76, "Worker process", ["One NodePool and one MemoryArbitrator per Worker JVM"], "#2563EB")
    panel(lines, 60, 250, 1080, 230, "RESERVATIONS", "#7C3AED", "#FBFBFE")
    node(lines, 90, 300, 320, 150, "Task execution", ["Per-attempt native budget", "Operator and spill accounting"], "#7C3AED")
    node(lines, 440, 300, 320, 150, "Shuffle and exchange", ["In-memory batches", "Disk service reservations"], "#D97706")
    node(lines, 790, 300, 310, 150, "Native cache", ["RAM / SSD cache budget", "Worker-local eviction"], "#0D9488")
    arrow(lines, 600, 480, 600, 520, "", "#7C3AED")
    panel(lines, 60, 520, 1080, 210, "PRESSURE RESPONSE", "#D97706", "#FFFCF5")
    node(lines, 90, 575, 320, 120, "Admission", ["Reject over-budget tasks", "Respect tenant quotas"], "#7C3AED")
    node(lines, 440, 575, 320, 120, "Spill", ["Task spill directory", "Language-specific file ownership"], "#D97706")
    node(lines, 790, 575, 310, 120, "Backpressure", ["Bounded queues", "Cancellation and drain"], "#D97706")
    node(lines, 80, 790, 1040, 90, "NativeMemoryGovernor and allocator", ["C++ allocation paths report through the governor; the JVM does not infer native usage"], "#0D9488")
    arrow(lines, 600, 730, 600, 790, "", "#0D9488")
    lines.append('<text x="600" y="930" font-size="14" fill="#64748B" text-anchor="middle">A second accounting layer must not be introduced merely to explain memory; the owning allocator remains authoritative.</text>')
    legend(lines, 960)
    lines.extend(svg_close())
    OUT.joinpath("memory-arbitration.svg").write_text("\n".join(lines), encoding="utf-8")


def tenant_isolation() -> None:
    simple_flow("tenant-isolation.svg",
        "Tenant isolation",
        "Identity, admission, placement, resource quotas, and audit use the same current tenant.",
        [
            ("Identity", "Authenticated principal · exact ACTIVE tenant · session context", "#2563EB"),
            ("Admission", "Tenant concurrency and resource limits · capability checks", "#7C3AED"),
            ("Placement", "Shared pools or dedicated Worker pools · no cross-pool fallback", "#0D9488"),
            ("Execution", "Resource grants · row policies and column masks · governed providers", "#D97706"),
            ("Audit", "Tenant-attributed query history · security events · lineage", "#7C3AED"),
        ],
        "An explicitly shared deployment uses only Workers without a dedicated pool assignment; unknown tenants are rejected.")


def governance_pipeline() -> None:
    h = 960
    lines = svg_open(h, "Data governance pipeline", "Lineage, quality rules, classification, and durable governance records.")
    title_block(lines, "Data governance pipeline", "Governance consumes execution facts and persists decisions beside the data authority.")
    node(lines, 80, 140, 1040, 76, "Sources", ["Query events and lineage · table columns and tags · quality rule definitions"], "#2563EB")
    arrow(lines, 600, 216, 600, 256, "", "#7C3AED")
    panel(lines, 60, 256, 1080, 220, "GOVERNANCE RUNTIME", "#7C3AED", "#FBFBFE")
    node(lines, 90, 308, 320, 140, "Lineage", ["Logical START and terminal events", "LOG or OpenLineage HTTP backend"], "#7C3AED")
    node(lines, 440, 308, 320, 140, "Quality", ["Rule evaluation at write gates", "Severity, table UUID, and row count"], "#D97706")
    node(lines, 790, 308, 310, 140, "Classification", ["Column tag bindings", "PII type and masking policy"], "#0D9488")
    arrow(lines, 600, 476, 600, 516, "", "#7C3AED")
    panel(lines, 60, 516, 1080, 210, "DURABLE RECORDS", "#0D9488", "#F7FEFC")
    node(lines, 90, 570, 320, 120, "PostgreSQL", ["Tag and policy bindings", "Audit and mutation receipts"], "#0D9488")
    node(lines, 440, 570, 320, 120, "Lineage backend", ["OpenLineage HTTP", "Configured outbox requirement"], "#0D9488")
    node(lines, 790, 570, 310, 120, "Query audit", ["Terminal state and failure flag", "Tenant and submitter"], "#0D9488")
    lines.append('<text x="600" y="800" font-size="14" fill="#64748B" text-anchor="middle">Missing authority, unsupported SQL, or an unavailable backend fails closed; governance never silently fabricates success.</text>')
    legend(lines, 930)
    lines.extend(svg_close())
    OUT.joinpath("governance-pipeline.svg").write_text("\n".join(lines), encoding="utf-8")


def optimization_surfaces() -> None:
    h = 930
    lines = svg_open(h, "Query optimization surfaces", "Planner rules, materialized-view rewrite, caches, admission, routing, statistics, and runtime filters.")
    title_block(lines, "Query optimization surfaces", "Only behaviour with production wiring and tests is described as available.")
    panel(lines, 60, 140, 520, 300, "IMPLEMENTED", "#7C3AED", "#FBFBFE")
    node(lines, 90, 195, 460, 90, "Calcite rules and cost model", ["Statistics-aware relational transformations"], "#7C3AED")
    node(lines, 90, 305, 460, 90, "Runtime filters", ["Same-task hash join filters and bounded literal rules"], "#D97706")
    panel(lines, 620, 140, 520, 300, "TARGET DESIGN", "#94A3B8", "#FFFFFF", dashed=True)
    node(lines, 650, 195, 460, 90, "Result cache and semantic cache", ["Evidence and invalidation design only"], "#94A3B8")
    node(lines, 650, 305, 460, 90, "Automatic cross-stage pruning", ["Build-side key-domain publisher"], "#94A3B8")
    arrow(lines, 600, 440, 600, 500, "", "#7C3AED")
    panel(lines, 60, 500, 1080, 210, "CONTROL SURFACES", "#0D9488", "#F7FEFC")
    node(lines, 90, 555, 320, 120, "Admission and throttling", ["Concurrency, resource, and tenant limits"], "#7C3AED")
    node(lines, 440, 555, 320, 120, "Routing and placement", ["Worker capabilities and locality"], "#0D9488")
    node(lines, 790, 555, 310, 120, "Materialized views", ["Rewrite from persisted definitions; lifecycle open"], "#7C3AED")
    lines.append('<text x="600" y="790" font-size="14" fill="#64748B" text-anchor="middle">A rule, cache, or rewrite class is not evidence of a supported plan shape; verify the bound plan and execution path.</text>')
    legend(lines, 900)
    lines.extend(svg_close())
    OUT.joinpath("optimization-surfaces.svg").write_text("\n".join(lines), encoding="utf-8")


def helm_topology() -> None:
    h = 960
    lines = svg_open(h, "Kubernetes deployment topology", "Ingress, Coordinator and Worker workloads, and external state.")
    title_block(lines, "Kubernetes deployment topology", "Namespaces, Services, and persistent dependencies are deployment-owned.")
    node(lines, 380, 140, 440, 76, "Ingress / load balancer", ["HTTPS :8080"], "#2563EB")
    arrow(lines, 600, 216, 600, 256, "", "#7C3AED")
    panel(lines, 60, 256, 1080, 250, "CONTROL-PLANE NAMESPACE", "#7C3AED", "#FBFBFE")
    node(lines, 190, 310, 380, 150, "Coordinator StatefulSet", ["Leader and follower replicas", "Lease and fencing", "Final catalog commit"], "#7C3AED")
    node(lines, 630, 310, 380, 150, "Coordinator Service", ["Stable client endpoint", "Readiness-gated routing"], "#0D9488")
    arrow(lines, 600, 506, 600, 546, "", "#7C3AED")
    panel(lines, 60, 546, 1080, 210, "WORKER NAMESPACE", "#D97706", "#FFFCF5")
    node(lines, 120, 600, 300, 120, "Worker pool", ["Autoscaled JVM + Native pods", "Advertised Flight endpoints"], "#D97706")
    node(lines, 450, 600, 300, 120, "Shuffle / TLS", ["Mutual TLS transport", "Object storage credentials"], "#0D9488")
    node(lines, 780, 600, 300, 120, "Secrets / KMS", ["Mounted files", "No plaintext credentials"], "#7C3AED")
    arrow(lines, 600, 756, 600, 796, "", "#0D9488")
    node(lines, 190, 796, 820, 84, "External state", ["PostgreSQL · object storage · optional ZooKeeper · OTel backend"], "#0D9488")
    legend(lines, 930)
    lines.extend(svg_close())
    OUT.joinpath("helm-topology.svg").write_text("\n".join(lines), encoding="utf-8")


def jni_ownership() -> None:
    h = 900
    lines = svg_open(h, "JNI ownership boundary", "Java control, JNI bridging, C++ execution, memory ownership, and errors.")
    title_block(lines, "JNI ownership boundary", "Handles and buffers have one owner; errors and lifecycle cross the bridge explicitly.")
    panel(lines, 60, 140, 1080, 200, "JAVA LAYER", "#7C3AED", "#FBFBFE")
    node(lines, 90, 195, 320, 110, "Task lifecycle", ["Admission · cancellation · drain", "Handle registry"], "#7C3AED")
    node(lines, 440, 195, 320, 110, "Connector callbacks", ["Page sinks and governed providers", "Resource callbacks"], "#0D9488")
    node(lines, 790, 195, 310, 110, "Result transport", ["Arrow Flight results", "Terminal reporting"], "#D97706")
    arrow(lines, 600, 340, 600, 380, "canonical v2 Protobuf / Arrow", "#7C3AED")
    panel(lines, 60, 380, 1080, 200, "JNI BRIDGE", "#0D9488", "#F7FEFC")
    node(lines, 90, 435, 470, 110, "Execution binding", ["Task request parsing", "Handle and incarnation validation"], "#0D9488")
    node(lines, 600, 435, 500, 110, "Error and lifecycle projection", ["Typed error mapping", "Owner-released receipts"], "#7C3AED")
    arrow(lines, 600, 580, 600, 620, "", "#D97706")
    panel(lines, 60, 620, 1080, 200, "C++ LAYER", "#D97706", "#FFFCF5")
    node(lines, 90, 675, 320, 110, "Velox execution", ["Operators and memory governor", "Task-scoped allocations"], "#D97706")
    node(lines, 440, 675, 320, 110, "Native shuffle / Flight", ["Arrow batches and tickets", "Process-shared services"], "#D97706")
    node(lines, 790, 675, 310, 110, "Spill and storage", ["Anonymous disk segments", "Owner-controlled cleanup"], "#0D9488")
    lines.append('<text x="600" y="870" font-size="14" fill="#64748B" text-anchor="middle">The Java heap never becomes the authority for native memory; a borrowed buffer is valid only for its documented callback window.</text>')
    lines.extend(svg_close())
    OUT.joinpath("jni-ownership.svg").write_text("\n".join(lines), encoding="utf-8")


def refresh_flow() -> None:
    simple_flow("refresh-flow.svg",
        "Dynamic table refresh",
        "Scheduling, planning, execution, and publication with explicit attempt state.",
        [
            ("Trigger", "Scheduled tick, manual command, or explicit refresh request", "#2563EB"),
            ("Plan", "Definition, source snapshot, refresh mode, and resource budget", "#7C3AED"),
            ("Execute", "Worker fragments produce output artifacts", "#D97706"),
            ("Publish", "Fenced generation publication and durable receipt", "#0D9488"),
            ("Record", "Attempt state, checkpoint, audit, and lineage", "#7C3AED"),
        ],
        "Continuous-query materialization remains unsupported; automatic refresh scheduling is a capability boundary, not a default promise.")

if __name__ == "__main__":
    main()
