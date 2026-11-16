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
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
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
          editUrl: 'https://github.com/pangrova-io/pangrova/tree/main/',
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
          editUrl: 'https://github.com/pangrova-io/pangrova/tree/main/website/',
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
    announcementBar: {
      id: 'active_development',
      content:
        'Pangrova is under active development. Review the <a href="/docs/getting-started/capability-status/">current capability status</a> before evaluating a workload.',
      backgroundColor: '#171336',
      textColor: '#ddd6fe',
      isCloseable: true,
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
          href: 'https://github.com/pangrova-io/pangrova/discussions',
          label: 'Community',
          position: 'left',
        },
        {
          href: 'https://github.com/pangrova-io/pangrova',
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
            {label: 'GitHub Discussions', href: 'https://github.com/pangrova-io/pangrova/discussions'},
            {label: 'Contributing', href: 'https://github.com/pangrova-io/pangrova/blob/main/CONTRIBUTING.md'},
            {label: 'Code of Conduct', href: 'https://github.com/pangrova-io/pangrova/blob/main/CODE_OF_CONDUCT.md'},
          ],
        },
        {
          title: 'Resources',
          items: [
            {label: 'Quickstart', to: '/docs/getting-started/quickstart/'},
            {label: 'Capability status', to: '/docs/getting-started/capability-status/'},
            {label: 'Roadmap', to: '/docs/getting-started/roadmap/'},
            {label: 'Releases', href: 'https://github.com/pangrova-io/pangrova/releases'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'Blog', to: '/blog/'},
            {label: 'Changelog', href: 'https://github.com/pangrova-io/pangrova/blob/main/CHANGELOG.md'},
            {label: 'License', href: 'https://github.com/pangrova-io/pangrova/blob/main/LICENSE'},
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
