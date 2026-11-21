import {execFileSync} from 'node:child_process';
import {readFileSync, statSync} from 'node:fs';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const require = createRequire(import.meta.url);
const coreRequire = createRequire(require.resolve('@docusaurus/core/package.json'));
const mdxRequire = createRequire(coreRequire.resolve('@docusaurus/mdx-loader'));
const {unified} = await import(mdxRequire.resolve('unified'));
const {default: remarkParse} = await import(mdxRequire.resolve('remark-parse'));
const {default: rehypeRaw} = await import(mdxRequire.resolve('rehype-raw'));
const markdown = unified().use(remarkParse);
const html = unified().use(rehypeRaw);

function walk(node, visit) {
  visit(node);
  node.children?.forEach((child) => walk(child, visit));
}

export function checkMarkdownImages(source, file, root) {
  const tree = markdown.parse(source);
  const definitions = new Map();
  const images = [];
  walk(tree, (node) => {
    if (node.type === 'definition' && !definitions.has(node.identifier)) {
      definitions.set(node.identifier, node.url);
    }
  });
  walk(tree, (node) => {
    const line = node.position?.start.line ?? 1;
    if (node.type === 'image') images.push({url: node.url, line});
    if (node.type === 'imageReference') {
      images.push({url: definitions.get(node.identifier), line});
    }
    if (node.type === 'html') {
      const fragment = html.runSync({type: 'root', children: [{type: 'raw', value: node.value}]});
      walk(fragment, (element) => {
        if (element.type === 'element' && element.tagName === 'img') {
          images.push({url: element.properties.src, line});
        }
      });
    }
  });

  const errors = [];
  let local = 0;
  let external = 0;
  for (const {url, line} of images) {
    const report = (reason) => errors.push(`${path.relative(root, file)}:${line}: ${reason}: ${url ?? '(empty)'}`);
    if (!url) {
      report('Image has no source');
      continue;
    }
    if (/^https?:\/\//i.test(url)) {
      external++;
      continue;
    }
    local++;
    if (url.startsWith('/') || /^[a-z][a-z\d+.-]*:/i.test(url)) {
      report('Use a repository-relative image path for GitHub and Docusaurus');
      continue;
    }
    let target;
    try {
      target = path.resolve(path.dirname(file), decodeURIComponent(url.split(/[?#]/)[0]));
    } catch {
      report('Invalid URL encoding');
      continue;
    }
    const relative = path.relative(root, target);
    if (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
      report('Image is outside the repository');
      continue;
    }
    if (!statSync(target, {throwIfNoEntry: false})?.isFile()) {
      report('Image file does not exist');
    }
  }
  return {local, external, errors};
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const root = fileURLToPath(new URL('../../', import.meta.url));
  const files = execFileSync('git', ['ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', '*.md', '*.mdx'], {cwd: root})
    .toString().split('\0').filter(Boolean);
  let local = 0;
  let external = 0;
  const errors = [];
  for (const relative of files) {
    const file = path.join(root, relative);
    if (!statSync(file, {throwIfNoEntry: false})?.isFile()) continue;
    const result = checkMarkdownImages(readFileSync(file, 'utf8'), file, root);
    local += result.local;
    external += result.external;
    errors.push(...result.errors);
  }
  if (errors.length) {
    console.error(errors.join('\n'));
    process.exitCode = 1;
  } else {
    console.log(`Documentation images: ${local} local references verified across ${files.length} Markdown files; ${external} external URLs not fetched.`);
  }
}
