import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { buildChangelog, collectHistory, generateReleaseNotes, generateSummary, parseVersion } from './generate-release-notes.mjs';

const script = fileURLToPath(new URL('./generate-release-notes.mjs', import.meta.url));

function repository(t) {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'clean-relay-notes-test-'));
  t.after(() => fs.rmSync(cwd, { recursive: true, force: true }));
  const git = (...args) => execFileSync('git', args, { cwd, encoding: 'utf8', windowsHide: true, stdio: 'pipe' }).trim();
  git('init', '--initial-branch=main');
  git('config', 'user.name', 'Release Test');
  git('config', 'user.email', 'release-test@example.invalid');
  git('config', 'commit.gpgsign', 'false');
  git('config', 'tag.gpgsign', 'false');
  const commit = (subject, body = '') => {
    fs.appendFileSync(path.join(cwd, 'fixture.txt'), `${subject}\n`);
    git('add', 'fixture.txt');
    git('commit', '-m', subject, '-m', body);
    return git('rev-parse', 'HEAD');
  };
  return { cwd, git, commit };
}

async function upstream(t, handle) {
  const server = http.createServer(handle);
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => { server.close(resolve); server.closeAllConnections(); }));
  return { AI_BASE_URL: `http://127.0.0.1:${server.address().port}/v1/`, AI_API_KEY: 'synthetic-key', AI_MODEL: 'test-model' };
}

const history = { tag: 'v0.1.0', previousTag: '', commits: [{ subject: 'feat: 添加清理', body: '' }] };

test('accepts stable and prerelease versions and rejects unsafe or ambiguous tags', () => {
  assert.deepEqual(parseVersion('v0.1.0'), { version: '0.1.0', tag: 'v0.1.0', prerelease: false });
  assert.equal(parseVersion('v1.2.3-rc.1').prerelease, true);
  for (const value of ['v01.2.3', 'v1.2', 'v1.2.3-rc.01', 'v1.2.3.4', 'v1.2.3+build', 'v1.2.3\n', 'v1.2.3;echo bad', '../v1.2.3', '-v1.2.3']) {
    assert.throws(() => parseVersion(value), /Version must/);
  }
});

test('first release includes the root commit and detects breaking changes in commit bodies', (t) => {
  const repo = repository(t);
  repo.commit('feat(cleaner): 添加清理');
  repo.commit('refactor(config): 调整配置', 'BREAKING CHANGE: 配置字段已移除');
  const result = collectHistory({ VERSION: '0.1.0' }, repo.cwd);
  assert.equal(result.commits.length, 2);
  assert.equal(result.previousTag, '');
  assert.match(result.stats, /1 file changed/);
  assert.match(buildChangelog(result.commits), /### 破坏性变更/);
  assert.match(buildChangelog(result.commits), /添加清理/);
});

test('release range excludes later commits and tags on unrelated branches', (t) => {
  const repo = repository(t);
  const root = repo.commit('feat: first');
  repo.git('tag', 'v0.1.0');
  repo.commit('fix: included');
  repo.git('tag', '-a', 'v0.2.0', '-m', 'release');
  repo.commit('fix: later');
  repo.git('tag', 'v0.3.0');
  repo.git('checkout', '-b', 'other', root);
  repo.commit('feat: unrelated');
  repo.git('tag', 'v99.0.0');
  const result = collectHistory({ CURRENT_TAG: 'v0.2.0' }, repo.cwd);
  assert.equal(result.previousTag, 'v0.1.0');
  assert.deepEqual(result.commits.map((commit) => commit.subject), ['fix: included']);
  assert.throws(() => collectHistory({ CURRENT_TAG: 'v0.2.0', PREV_VERSION: 'v99.0.0' }, repo.cwd));
});

test('metadata checks the tagged commit and marks prereleases', (t) => {
  const repo = repository(t);
  repo.commit('feat: first');
  repo.git('tag', 'v0.1.0-rc.1');
  const output = path.join(repo.cwd, 'metadata.out');
  const options = { cwd: repo.cwd, env: { ...process.env, RELEASE_TAG: 'v0.1.0-rc.1', GITHUB_OUTPUT: output }, windowsHide: true, stdio: 'pipe' };
  execFileSync(process.execPath, [script, '--metadata'], options);
  assert.equal(fs.readFileSync(output, 'utf8'), 'version=0.1.0-rc.1\nprerelease=true\n');
  repo.commit('fix: later');
  assert.throws(() => execFileSync(process.execPath, [script, '--metadata'], options));
});

test('missing AI configuration writes a complete fallback without making a network request', async (t) => {
  const repo = repository(t);
  repo.commit('feat: initial release');
  const envFile = path.join(repo.cwd, 'github-env.out');
  const result = await generateReleaseNotes({
    cwd: repo.cwd, env: { VERSION: '0.1.0', GITHUB_ENV: envFile },
    fetchImpl: () => assert.fail('Unconfigured AI must not make a request'),
  });
  assert.equal(result.fallback, true);
  assert.match(result.content, /initial release/);
  assert.match(result.content, /Windows 10\/11 x64/);
  assert.match(fs.readFileSync(envFile, 'utf8'), /^AI_NOTES_FALLBACK=true\n/);
  assert.equal(fs.readFileSync(result.output, 'utf8'), result.content);
});

test('supports JSON gateways and sends only the configured request options', async (t) => {
  let requestBody;
  const env = await upstream(t, async (request, response) => {
    assert.equal(request.url, '/v1/chat/completions');
    assert.equal(request.headers.authorization, 'Bearer synthetic-key');
    let body = '';
    for await (const chunk of request) body += chunk;
    requestBody = JSON.parse(body);
    response.setHeader('Content-Type', 'application/json');
    response.end(JSON.stringify({ choices: [{ message: { content: '```markdown\n## 版本简介\n\n发布内容。\n```' }, finish_reason: 'stop' }] }));
  });
  const content = await generateSummary(history, { ...env, AI_MAX_TOKENS: '4096', AI_REASONING_EFFORT: 'low' });
  assert.equal(content, '## 版本简介\n\n发布内容。');
  assert.equal(requestBody.max_tokens, 4096);
  assert.equal(requestBody.reasoning_effort, 'low');
  assert.equal(requestBody.stream, true);
  assert.equal(requestBody.model, 'test-model');
});

test('handles split UTF-8 and CRLF SSE events and writes AI notes plus the full changelog', async (t) => {
  const repo = repository(t);
  repo.commit('feat: retained commit');
  const env = await upstream(t, (_request, response) => {
    response.setHeader('Content-Type', 'text/event-stream');
    const event = Buffer.from('data: {"choices":[{"delta":{"content":"## 新功能\\n\\n清理请求。"}}]}\r\n\r\n');
    const boundary = event.indexOf(Buffer.from('清')) + 1;
    response.write(event.subarray(0, boundary));
    setImmediate(() => {
      response.write(event.subarray(boundary, event.length - 1));
      setImmediate(() => response.end(event.subarray(event.length - 1) + 'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\r\n\r\ndata: [DONE]\r\n\r\n'));
    });
  });
  const result = await generateReleaseNotes({ cwd: repo.cwd, env: { ...env, VERSION: '0.1.0' } });
  assert.equal(result.fallback, false);
  assert.match(result.content, /清理请求。/);
  assert.match(result.content, /<details>/);
  assert.match(result.content, /retained commit/);
});

test('AI failure, truncation and incomplete streams fall back without exposing upstream data', async (t) => {
  const repo = repository(t);
  repo.commit('fix: fallback commit');
  for (const scenario of ['http', 'truncated', 'incomplete', 'empty', 'invalid-json']) {
    await t.test(scenario, async (subtest) => {
      const env = await upstream(subtest, (_request, response) => {
        if (scenario === 'http') { response.writeHead(503); response.end('synthetic-private-error'); return; }
        if (scenario === 'invalid-json') { response.end('synthetic-private-error'); return; }
        if (scenario === 'incomplete') {
          response.setHeader('Content-Type', 'text/event-stream');
          response.end('data: {"choices":[{"delta":{"content":"synthetic-private-error"}}]}\n\n');
          return;
        }
        response.end(JSON.stringify({ choices: [{ message: { content: scenario === 'empty' ? '' : 'synthetic-private-error' }, finish_reason: scenario === 'truncated' ? 'length' : 'stop' }] }));
      });
      const result = await generateReleaseNotes({ cwd: repo.cwd, env: { ...env, VERSION: '0.1.0' } });
      assert.equal(result.fallback, true);
      assert.match(result.content, /fallback commit/);
      assert.doesNotMatch(result.content, /synthetic-private-error|synthetic-key/);
    });
  }
});

test('an idle stream times out and falls back', async (t) => {
  const repo = repository(t);
  repo.commit('fix: timeout fallback');
  const env = await upstream(t, (_request, response) => {
    response.setHeader('Content-Type', 'text/event-stream');
    response.write(': keepalive\n\n');
  });
  const result = await generateReleaseNotes({ cwd: repo.cwd, env: { ...env, VERSION: '0.1.0', AI_IDLE_TIMEOUT_MS: '100' } });
  assert.equal(result.fallback, true);
  assert.match(result.content, /AI 请求超时/);
  assert.match(result.content, /timeout fallback/);
});
