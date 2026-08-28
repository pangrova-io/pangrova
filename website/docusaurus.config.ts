import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Pangrova',
  tagline: 'The Distributed SQL Query Engine Built for Lakehouse',
  favicon: 'img/favicon.ico',

  url: 'https://pangrova-project.github.io',
  baseUrl: '/pangrova/',

  organizationName: 'pangrova-project',
  projectName: 'pangrova',

  onBrokenLinks: 'throw',

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
          editUrl: 'https://github.com/pangrova-project/pangrova/tree/main/',
          showLastUpdateTime: true,
          showLastUpdateAuthor: true,
          numberPrefixParser: false,
          exclude: [
            'internal/**',
          ],
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
          editUrl: 'https://github.com/pangrova-project/pangrova/tree/main/website/',
        },
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/logo.png',
    navbar: {
      title: 'Pangrova',
      logo: {
        alt: 'Pangrova Logo',
        src: 'img/logo.png',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docs',
          position: 'left',
          label: 'Documentation',
        },
        {to: '/blog', label: 'Blog', position: 'left'},
        {
          href: 'https://github.com/pangrova-project/pangrova',
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
            {label: 'Getting Started', to: '/docs/getting-started/quickstart'},
            {label: 'SQL Reference', to: '/docs/sql-reference/queries'},
            {label: 'Architecture', to: '/docs/architecture/overview'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub Discussions', href: 'https://github.com/pangrova-project/pangrova/discussions'},
            {label: 'Contributing', href: 'https://github.com/pangrova-project/pangrova/blob/main/CONTRIBUTING.md'},
            {label: 'Code of Conduct', href: 'https://github.com/pangrova-project/pangrova/blob/main/CODE_OF_CONDUCT.md'},
          ],
        },
        {
          title: 'More',
          items: [
            {label: 'Blog', to: '/blog'},
            {label: 'Changelog', href: 'https://github.com/pangrova-project/pangrova/blob/main/CHANGELOG.md'},
            {label: 'Releases', href: 'https://github.com/pangrova-project/pangrova/releases'},
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
