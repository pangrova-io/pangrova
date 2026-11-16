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

Pull requests validate the site without deployment. The public site is published
at https://pangrova-b9a20lmw2et.qoder.website. The GitHub Pages workflow remains
available when the repository enables Pages with GitHub Actions as the source; it
builds with the `/pangrova/` base path, while the Qoder publication builds with
`PANGROVA_SITE_BASE_URL=/` and `PANGROVA_SITE_URL=<public site URL>`. No local
`gh-pages` branch deployment is needed.
