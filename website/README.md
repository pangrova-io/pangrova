# Pangrova website

The Docusaurus site publishes `../docs` and `blog/`. Use Node.js 22 and pnpm 10,
matching `.github/workflows/deploy-website.yml`. `pnpm-lock.yaml` is the dependency
lockfile; do not introduce a second package-manager lockfile.

```bash
cd website
pnpm install --frozen-lockfile
pnpm start
```

Before submitting a documentation or site change:

```bash
pnpm run typecheck
pnpm run build
```

Broken routes, Markdown targets and anchors fail the build. Navigation is owned
by `sidebars.ts`; documentation format is described in
[the documentation guide](../docs/contributing/documentation.md).

`static/img/` is the shared image source for this site and the root README. The
root README embeds `static/img/architecture-overview.png`; its editable source is
`static/img/architecture-overview.svg`. Retain `static/.nojekyll`, which is a
GitHub Pages deployment marker.

The official site is https://pangrova-io.github.io/pangrova/, deployed by
`.github/workflows/deploy-website.yml` on every push to `main` that changes `docs/`
or `website/`. For a private repository an organization owner must enable Pages once
(Settings → Pages → Source: GitHub Actions), which requires a plan that
includes Pages; the deployment token cannot create the site itself. Pull
requests build and typecheck the site without deploying. Builds default to the
`/pangrova/` base path; `PANGROVA_SITE_URL` and `PANGROVA_SITE_BASE_URL` override
the URL and base path for another host. No `gh-pages` branch is used.
