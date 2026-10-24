---
title: "Documentation Guide"
description: "Structure, naming, accuracy, and validation rules for Pangrova documentation"
---

# Documentation Guide

Pangrova documentation is organized by reader task. Published guides, architecture references,
internal roadmap material, runtime configuration, and deployment assets have separate homes.
“Internal” excludes content from the website only; tracked files remain public when
the repository is made public. Do not store confidential material there.

## Directory ownership

| Path | Purpose |
|---|---|
| `docs/getting-started/` | Installation, clients, catalogs, and connector onboarding |
| `docs/sql-reference/` | Executable SQL behavior and runtime constraints |
| `docs/data-types/` | Type syntax, storage mapping, and connector limitations |
| `docs/indexing/` | Index DDL and index-type behavior |
| `docs/configuration/` | Verified configuration model and key reference |
| `docs/deployment/` | Deployment procedures that consume assets from `deploy/` and `etc/` |
| `docs/architecture/` | Current system design, boundaries, invariants, and known gaps |
| `docs/operations/` | Day-2 procedures, runbooks, game-day scenarios, and templates |
| `docs/contributing/` | Developer, test, and documentation workflows |
| `docs/internal/` | Non-published audits and roadmap inputs; never a user-facing capability contract |
| `etc/` | Runtime configuration templates used by launch scripts |
| `deploy/` | Docker and Helm deployment assets |

## Naming

- Use lowercase kebab-case file and directory names.
- Use `index.md` for a section landing page; do not use `README.md` as a site document ID.
- Do not encode sidebar order in file names. `website/sidebars.ts` owns navigation order.
- Name files by subject (`distributed-writes.md`), not by document type suffixes such as
  `-design`, `-guide`, or `-architecture` when the parent directory already provides that context.
- Keep dates only in real, immutable records such as verified incident or game-day reports.
- Put `Status: Draft` or `Status: Target architecture` near the title when a document
  primarily describes future design. Mixed documents must mark proposed sections locally.

## Accuracy

For feature claims, trace the full chain that applies:

```text
SQL/API -> parser -> planner -> dispatch -> connector/JNI/native -> result -> integration/E2E test
```

An SPI, AST node, parser rule, or isolated implementation is not sufficient evidence of support.
Use explicit status language for partial wiring and runtime dependencies. Do not publish synthetic
benchmark numbers, incidents, people, environments, or action items as real operational records.

Sources of truth, in descending order:

1. Production code and wiring.
2. Focused integration and E2E tests.
3. Typed configuration definitions and bundled defaults.
4. `.pangrova/rules/` for architectural and repository policy.
5. Architecture documentation and internal roadmaps.

## Format

- Add YAML front matter with a concise `title` and `description` to every published Markdown file.
- Use exactly one level-one heading matching the title.
- Prefer relative links ending in `.md`; the website build validates targets and anchors.
- Mark shell, SQL, YAML, Java, and C++ fences with a language.
- Use tables only for exact mappings; use prose for rationale and constraints.
- Keep examples executable and free of real credentials, personal paths, and unverified output.

## Validation

Repository policy is discovered through `AGENTS.md` and `.pangrova/rules/`.
`.pangrova/setup.sh --check` validates this canonical layout without creating any
tool-specific adapter directories.

Run the closest documentation checks after every structural change:

```bash
cd website
pnpm install --frozen-lockfile
pnpm run typecheck
pnpm run build

cd ..
.pangrova/scripts/check-doc-class-refs.sh
.pangrova/setup.sh --check
```

The website treats broken routes and Markdown links as build failures. When renaming a document,
update source comments, README links, the sidebar, and every relative link in the same change.
