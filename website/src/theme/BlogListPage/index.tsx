import React from 'react';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import BlogLayout from '@theme/BlogLayout';
import BlogListPaginator from '@theme/BlogListPaginator';
import BlogPostItems from '@theme/BlogPostItems';
import BlogListPageStructuredData from '@theme/BlogListPage/StructuredData';
import type {Props} from '@theme/BlogListPage';
import styles from './styles.module.css';

export default function BlogListPage({sidebar, metadata, items}: Props) {
  const {
    siteConfig: {title: siteTitle},
  } = useDocusaurusContext();
  const pageTitle = metadata.blogTitle || 'Blog';
  const fullTitle = pageTitle.includes(siteTitle)
    ? pageTitle
    : `${pageTitle} | ${siteTitle}`;

  return (
    <>
      <Head>
        <title>{fullTitle}</title>
        <meta name="description" content={metadata.blogDescription} />
      </Head>
      <BlogListPageStructuredData sidebar={sidebar} metadata={metadata} items={items} />
      <BlogLayout sidebar={sidebar}>
        <header className={styles.hero}>
          <p className={styles.eyebrow}>Engineering blog</p>
          <h1 className={styles.title}>Pangrova Blog</h1>
          <p className={styles.lead}>
            Engineering updates, release notes, and deep dives into Pangrova internals.
          </p>
        </header>
        <BlogPostItems items={items} />
        <BlogListPaginator metadata={metadata} />
      </BlogLayout>
    </>
  );
}
