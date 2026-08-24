import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import styles from './index.module.css';

const features = [
  {
    icon: '⚡',
    title: 'Native Performance',
    description: 'C++ Velox vectorized execution with Arrow zero-copy JNI — bare-metal throughput without JVM overhead.',
  },
  {
    icon: '🔓',
    title: 'Open Formats',
    description: 'First-class Apache Iceberg & Paimon connectors. Your data stays in open table formats — no vendor lock-in.',
  },
  {
    icon: '🤖',
    title: 'AI-Native SQL',
    description: 'Vector search, hybrid retrieval, in-DB embeddings, LLM UDFs — AI is a SQL primitive, not an external service.',
  },
  {
    icon: '📈',
    title: 'Elastic Scale',
    description: 'Multi-active Coordinator HA (no SPOF) + auto-scaling Worker pool. Scale from laptop to petabytes.',
  },
  {
    icon: '🛡️',
    title: 'Enterprise Security',
    description: 'JWT / LDAP / OAuth2 authentication, RBAC, column masking, row-level filtering — production-ready.',
  },
  {
    icon: '🔭',
    title: 'Full Observability',
    description: 'OpenTelemetry distributed tracing, Prometheus metrics, structured logging — built in from day one.',
  },
];

const techStack = [
  'Apache Calcite',
  'Meta Velox',
  'Apache Arrow',
  'Substrait',
  'Apache Iceberg',
  'Apache Paimon',
  'Arrow Flight',
  'OpenTelemetry',
];

const sqlExamples = {
  analytics: `SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue
FROM iceberg.sales.orders
WHERE order_date >= TIMESTAMP '2024-01-01'
GROUP BY region
ORDER BY revenue DESC;`,
  ai: `-- Vector similarity search with ANN acceleration
SELECT id, vector_score(embedding, ai_embed('bge-m3', 'noise cancelling'))
FROM products
ORDER BY vector_score DESC
LIMIT 10;`,
  timeTravel: `-- Query data as it existed at a specific point in time
SELECT * FROM iceberg.analytics.events
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-06-01 12:00:00';`,
};

function HeroSection() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <img src="img/logo.png" alt="Pangrova Logo" className={styles.heroLogo} />
        <h1 className={styles.heroTitle}>{siteConfig.title}</h1>
        <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
        <p className={styles.heroDescription}>
          Calcite Optimization + Arrow Zero-Copy + Velox Native Execution
        </p>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/guide/getting-started">
            Get Started →
          </Link>
          <Link
            className="button button--outline button--lg"
            style={{color: 'white', borderColor: 'white', marginLeft: '1rem'}}
            href="https://github.com/pangrova-project/pangrova">
            GitHub ★
          </Link>
        </div>
      </div>
    </header>
  );
}

function FeaturesSection() {
  return (
    <section className={styles.features}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Why Pangrova?</h2>
        <div className={styles.featureGrid}>
          {features.map((feature, idx) => (
            <div key={idx} className={clsx('card', styles.featureCard)}>
              <div className={styles.featureIcon}>{feature.icon}</div>
              <h3>{feature.title}</h3>
              <p>{feature.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function ArchitectureSection() {
  return (
    <section className={styles.architectureSection}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Architecture</h2>
        <div className={styles.archImageWrapper}>
          <img
            src="img/architecture.png"
            alt="Pangrova Architecture"
            className={styles.archImage}
          />
        </div>
      </div>
    </section>
  );
}

function CodeShowcase() {
  return (
    <section className={styles.codeSection}>
      <div className="container">
        <h2 className={styles.sectionTitle}>SQL in Action</h2>
        <div className={styles.codeGrid}>
          <div>
            <h4>📊 Lakehouse Analytics</h4>
            <CodeBlock language="sql">{sqlExamples.analytics}</CodeBlock>
          </div>
          <div>
            <h4>🤖 AI-Native Search</h4>
            <CodeBlock language="sql">{sqlExamples.ai}</CodeBlock>
          </div>
          <div>
            <h4>⏰ Time Travel</h4>
            <CodeBlock language="sql">{sqlExamples.timeTravel}</CodeBlock>
          </div>
        </div>
      </div>
    </section>
  );
}

function TechStackSection() {
  return (
    <section className={styles.techSection}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Built On</h2>
        <div className={styles.techBadges}>
          {techStack.map((tech, idx) => (
            <span key={idx} className={styles.techBadge}>{tech}</span>
          ))}
        </div>
      </div>
    </section>
  );
}

function QuickStartSection() {
  return (
    <section className={styles.quickStart}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Quick Start</h2>
        <CodeBlock language="bash">
{`git clone https://github.com/pangrova-project/pangrova.git
cd pangrova && mvn clean package -DskipTests
./bin/pangrova-standalone.sh          # Start engine
./bin/pangrova-cli.sh                 # Interactive SQL`}
        </CodeBlock>
      </div>
    </section>
  );
}

export default function Home(): React.JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout title="Home" description={siteConfig.tagline}>
      <HeroSection />
      <main>
        <FeaturesSection />
        <ArchitectureSection />
        <CodeShowcase />
        <TechStackSection />
        <QuickStartSection />
      </main>
    </Layout>
  );
}

