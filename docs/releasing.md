# 发布 CleanRelay

当前发布目标是 Windows amd64 独立二进制，构建使用 `CGO_ENABLED=0`、`GOAMD64=v1`。发行文件不依赖 Go、Node.js 或 C 运行时安装。

## 工作流

流程参考 LinguaFlow，按 CleanRelay 的 Windows 命令行程序范围配置：

1. `main` 推送及 Pull Request 触发 [CI](../.github/workflows/ci.yml)：Go 格式、`go vet`、Windows 全量竞态测试（含真实 Ctrl+C 退出）、发布说明脚本测试及二进制验证。
2. 推送 `v*` 标签触发 [Release](../.github/workflows/release.yml)，先复用同一套 CI。
3. 校验标签和检出的提交，构建带对应版本号的 exe，并校验示例配置。
4. 从上一祖先版本标签到当前标签提取提交，生成 AI 概述并附上完整提交列表；首次发布包含根提交。缺少 AI 配置、请求失败、超时或输出不完整时，回退为完整提交列表，继续创建草稿。
5. 创建 GitHub Release **草稿**，由维护者检查说明和附件后点击 **Publish release** 正式发布。

标签支持 `vX.Y.Z` 和 `vX.Y.Z-<prerelease>`，例如 `v0.1.0`、`v0.1.0-rc.1`。版本数字及纯数字预发布段不能有多余前导零，不接受 `+build` 元数据。带预发布段的标签自动标记为 Pre-release；exe 的 `-version` 输出省略标签开头的 `v`。

Release 只上传以下四个文件；本地配置和日志不在上传名单中：

| 附件 | 用途 |
| --- | --- |
| `clean-relay.exe` | Windows amd64 程序 |
| `clean-relay.exe.sha256` | exe 的 SHA256 校验值 |
| `clean-relay.example.json` | 不含密钥的示例配置 |
| `README.md` | 独立 Windows 启动说明 |

## GitHub 配置

在仓库 **Settings → Secrets and variables → Actions** 设置 AI 接口。接口应兼容 Chat Completions，`AI_BASE_URL` 包含 API 前缀（如 `/v1`），脚本会追加 `/chat/completions`。

| 名称 | 存放位置 | 说明 |
| --- | --- | --- |
| `AI_BASE_URL` | Secret | 例如 `https://your-gateway.example/v1` |
| `AI_API_KEY` | Secret | 用于生成发布说明的 API Key |
| `AI_MODEL` | Variable | 网关支持的模型名称 |
| `AI_MAX_TOKENS` | Variable，可选 | 正整数；留空使用上游默认输出上限 |
| `AI_REASONING_EFFORT` | Variable，可选 | 上游支持的推理档位；留空不发送 |
| `AI_IDLE_TIMEOUT_MS` | Variable，可选 | 流式空闲超时，默认 90000 毫秒，最大 300000 毫秒 |
| `CF_ACCESS_CLIENT_ID` | Secret，可选 | AI 网关启用 Cloudflare Access 时填写 |
| `CF_ACCESS_CLIENT_SECRET` | Secret，可选 | 与 Client ID 一起配置 |

前三项需要全部设置才会请求 AI。AI 请求仅发送项目说明、版本号和提交信息，不发送源码、请求样例或本地配置；默认使用流式输出，也兼容返回普通 JSON 的网关。单次生成总时限为 5 分钟。

创建 Release 使用工作流自带的 `GITHUB_TOKEN`，只有发布 job 申请 `contents: write`，不需要另外配置发布用 PAT。CI 和发布说明测试只访问本地假上游，不使用这些 AI Secrets。

## 本地验证

需要 Go 1.26、PowerShell 7；竞态检测需要 GCC，发布说明脚本测试需要 Node.js 22 或更新版本（CI 使用 Node.js 24）。在仓库根目录执行：

```powershell
go test ./...
go vet ./...
go test -race -count=1 -timeout=10m ./...
node --test .github/scripts/generate-release-notes.test.mjs
.\scripts\check-release.ps1
```

`check-release.ps1` 在独立临时目录内构建，验证版本、示例配置、目标平台、CGO、CPU 基线、SHA256 和启动说明，并检查原有本地配置及构建环境变量得到保留。

按拟发布版本生成本地发行文件，例如首版 `0.1.0`：

```powershell
.\scripts\build.ps1 -Version 0.1.0 -OutputDirectory dist/release
.\dist\release\clean-relay.exe -version
.\dist\release\clean-relay.exe -config .\dist\release\clean-relay.example.json -check-config
```

预览当前 HEAD 的发布说明：

```powershell
$env:VERSION = '0.1.0'
$env:OUTPUT_PATH = 'dist/release-notes-preview.md'
node .github/scripts/generate-release-notes.mjs
Remove-Item Env:VERSION, Env:OUTPUT_PATH
```

没有配置 AI 时，这个命令可离线验证提交列表回退。若要预览已有标签，使用 `CURRENT_TAG=v0.1.0`；`PREV_VERSION` 可指定较早的祖先标签。脚本验证 Git 版本范围失败时会终止，避免生成错误版本的说明。

## 正式发布

先将发布工作流、脚本和文档提交到 `main`，确认本地改动已提交、CI 通过，再为相同提交创建标签。以下以 `v0.1.0` 为例，实际发布时替换为目标版本：

```powershell
git status --short
git push origin main
git tag -a v0.1.0 -m 'CleanRelay v0.1.0'
git push origin v0.1.0
```

打开 GitHub Actions 中的 **Release** 运行。成功后，运行摘要包含 Release 草稿链接及 AI 是否回退；构建文件作为 Actions artifact 保留 14 天，测试日志保留 7 天。

在草稿中检查版本和附件，核对 AI 说明与实际提交，补充必要的使用或升级说明后发布。可选会话修复的真实渠道验收状态仍以 [README](../README.md#开发) 为准；自动化通过不代表已经完成真实渠道验收。

工作流失败时可在相同标签上选择 **Re-run failed jobs**。已有草稿可以更新，AI 说明也会重新生成；已经正式发布的版本会被拒绝覆盖，后续修改使用新版本标签。草稿尚未公开时，也可直接编辑其中回退的提交列表说明后发布。
