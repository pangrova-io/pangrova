import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {fileURLToPath} from 'node:url';
import {checkMarkdownImages} from './check-doc-images.mjs';

function fixture(t) {
  const root = mkdtempSync(path.join(os.tmpdir(), 'pangrova-doc-images-'));
  t.after(() => rmSync(root, {recursive: true, force: true}));
  const images = path.join(root, 'website/static/img');
  mkdirSync(images, {recursive: true});
  writeFileSync(path.join(images, 'layout.svg'), '<svg xmlns="http://www.w3.org/2000/svg"/>');
  writeFileSync(path.join(images, 'layout copy.svg'), '<svg xmlns="http://www.w3.org/2000/svg"/>');
  return {root, file: path.join(root, 'docs/getting-started/quickstart.md')};
}

const image = '../../website/static/img/layout.svg';

test('accepts relative images at different document depths', (t) => {
  const {root, file} = fixture(t);
  for (const [document, url] of [
    [file, image],
    [path.join(root, 'docs/operations/game-day/runbooks/disk.md'), '../../../../website/static/img/layout.svg'],
    [path.join(root, 'website/blog/post.md'), '../static/img/layout.svg'],
    [path.join(root, 'README.md'), 'website/static/img/layout.svg'],
  ]) {
    assert.deepEqual(checkMarkdownImages(`![Layout](${url})`, document, root), {local: 1, external: 0, errors: []});
  }
});

test('rejects the quickstart site-root regression even when the asset exists', (t) => {
  const {root, file} = fixture(t);
  const result = checkMarkdownImages('\n![pangrova-{version}/](/img/layout.svg)', file, root);
  assert.equal(result.errors.length, 1);
  assert.match(result.errors[0], /quickstart.md:2: Use a repository-relative/);
});

test('rejects missing files and directory targets', (t) => {
  const {root, file} = fixture(t);
  const result = checkMarkdownImages(`![Missing](${image}x)\n![Directory](../../website/static/img)`, file, root);
  assert.equal(result.errors.length, 2);
  assert.ok(result.errors.every((error) => error.includes('does not exist')));
});

test('checks full, collapsed, and shortcut image references', (t) => {
  const {root, file} = fixture(t);
  const source = `![Layout][diagram]\n![diagram][]\n![diagram]\n\n[diagram]: ${image} "Layout"`;
  assert.deepEqual(checkMarkdownImages(source, file, root), {local: 3, external: 0, errors: []});
  assert.equal(checkMarkdownImages(source.replace(image, '/img/layout.svg'), file, root).errors.length, 3);
});

test('checks HTML images and ignores HTML comments', (t) => {
  const {root, file} = fixture(t);
  const source = `<p><img\n src="${image}" alt="Layout" /></p>\n\n<!-- <img src="/img/ignored.svg" /> -->`;
  assert.deepEqual(checkMarkdownImages(source, file, root), {local: 1, external: 0, errors: []});
  assert.equal(checkMarkdownImages('<img src="/img/layout.svg">', file, root).errors.length, 1);
  assert.equal(checkMarkdownImages('<img alt="Missing source">', file, root).errors.length, 1);
});

test('ignores fenced and inline code examples', (t) => {
  const {root, file} = fixture(t);
  const source = '```md\n![Example](/img/missing.svg)\n```\n\n`![Example](/img/missing.svg)`\n\n```html\n<img src="/img/missing.svg">\n```';
  assert.deepEqual(checkMarkdownImages(source, file, root), {local: 0, external: 0, errors: []});
});

test('resolves encoded filenames, titles, queries, and fragments', (t) => {
  const {root, file} = fixture(t);
  const source = `![Layout](../../website/static/img/layout%20copy.svg?v=1#diagram "Title")\n![Layout](<../../website/static/img/layout copy.svg>)`;
  assert.deepEqual(checkMarkdownImages(source, file, root), {local: 2, external: 0, errors: []});
  assert.match(checkMarkdownImages('![Bad](broken%ZZ.svg)', file, root).errors[0], /Invalid URL encoding/);
});

test('does not fetch external images or badges', (t) => {
  const {root, file} = fixture(t);
  assert.deepEqual(checkMarkdownImages('![Badge](https://example.invalid/badge.svg)', file, root), {local: 0, external: 1, errors: []});
});

test('rejects nonportable sources and repository escapes', (t) => {
  const {root, file} = fixture(t);
  for (const url of ['../../../outside.svg', 'file:///tmp/layout.svg', '//example.invalid/image.svg', 'data:image/png;base64,AAAA']) {
    assert.equal(checkMarkdownImages(`![Layout](${url})`, file, root).errors.length, 1, url);
  }
});

test('vectorizer emits a portable image and does not rewrite it on a second run', (t) => {
  const {root, file} = fixture(t);
  mkdirSync(path.dirname(file), {recursive: true});
  writeFileSync(file, '# Layout\n\n```text\npangrova-{version}/\n├── bin/\n└── lib/\n```\n');
  const generator = fileURLToPath(new URL('./vectorize-diagrams.py', import.meta.url));
  execFileSync('python3', [generator], {cwd: root});
  const generated = readFileSync(file, 'utf8');
  assert.match(generated, /\]\(\.\.\/\.\.\/website\/static\/img\/diagrams\/generated\//);
  assert.deepEqual(checkMarkdownImages(generated, file, root), {local: 1, external: 0, errors: []});
  execFileSync('python3', [generator], {cwd: root});
  assert.equal(readFileSync(file, 'utf8'), generated);
});
