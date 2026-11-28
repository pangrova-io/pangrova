import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const siteUrl = process.env.PANGROVA_SITE_URL ?? 'https://pangrova-io.github.io';
const siteBaseUrl = process.env.PANGROVA_SITE_BASE_URL ?? '/pangrova/';

const config: Config = {
  title: 'Pangrova',
  tagline: 'Distributed SQL for open lakehouses',
  favicon: 'img/favicon.ico',

  url: siteUrl,
  baseUrl: siteBaseUrl,
  trailingSlash: true,

  organizationName: 'pangrova-io',
  projectName: 'pangrova',

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  onDuplicateRoutes: 'throw',

  headTags: [
    {
      tagName: 'meta',
      attributes: {
        name: 'keywords',
        content:
          'Pangrova, distributed SQL, lakehouse, Apache Iceberg, Apache Paimon, Apache Calcite, Velox, Arrow Flight, vector search',
      },
    },
  ],

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
    localeConfigs: {
      en: {label: 'English', direction: 'ltr'},
    },
  },

  markdown: {
    mermaid: true,
    format: 'detect',
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  themes: [
    '@docusaurus/theme-mermaid',
    [
      '@easyops-cn/docusaurus-search-local',
      {
        // Query hashes are misrouted by the preview server under a non-root base path.
        hashed: 'filename',
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
        indexDocs: true,
        indexBlog: true,
        indexPages: false,
        docsDir: '../docs',
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          path: '../docs',
          sidebarPath: './sidebars.ts',
          showLastUpdateTime: true,
          showLastUpdateAuthor: true,
          numberPrefixParser: false,
        },
        blog: {
          showReadingTime: true,
          blogTitle: 'Pangrova Blog',
          blogDescription: 'Engineering updates, release notes, and deep dives into Pangrova internals',
          feedOptions: {
            type: ['rss', 'atom'],
            title: 'Pangrova Blog',
            copyright: `Copyright ${new Date().getFullYear()} The Pangrova Authors`,
          },
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/logo.png',
    docs: {
      sidebar: {
        hideable: true,
        autoCollapseCategories: false,
      },
    },
    tableOfContents: {
      minHeadingLevel: 2,
      maxHeadingLevel: 4,
    },
    navbar: {
      title: 'Pangrova',
      logo: {
        alt: 'Pangrova Logo',
        src: 'img/logo-transparent.png',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          position: 'left',
          label: 'Documentation',
        },
        {to: '/blog/', label: 'Blog', position: 'left'},

        {
          href: 'https://github.com/pangrova-io',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Getting Started', to: '/docs/getting-started/quickstart/'},
            {label: 'SQL Reference', to: '/docs/sql-reference/queries/'},
            {label: 'Architecture', to: '/docs/architecture/overview/'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub organization', href: 'https://github.com/pangrova-io'},
            {label: 'Documentation', to: '/docs/'},
          ],
        },
        {
          title: 'Resources',
          items: [
            {label: 'Quickstart', to: '/docs/getting-started/quickstart/'},
            {label: 'Roadmap', to: '/docs/getting-started/roadmap/'},
          ],
        },

      ],
      copyright: `Copyright © ${new Date().getFullYear()} The Pangrova Authors. Apache 2.0 Licensed.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['java', 'sql', 'bash', 'yaml', 'protobuf', 'cmake', 'properties'],
    },
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
