# CleanRelay（净流）

CleanRelay 是使用 Go 标准库实现的轻量请求清理代理：客户端把请求发给它，它删除上游不兼容的 JSON 字段后再转发，并把响应原样带回。

```mermaid
flowchart LR
    Client[本机客户端] -->|HTTP JSON / SSE| Relay["CleanRelay · 127.0.0.1:17880"]
    Relay -->|清理后的请求| Upstream[上游服务]
    Upstream -->|JSON / SSE| Relay
    Relay -->|原样返回| Client
```

## 快速开始

构建需要 Go 1.26 和 PowerShell 7：

```powershell
.\scripts\build.ps1
```

产物位于 `dist`：exe、示例配置、独立启动说明（`docs/windows-usage.md` 的副本）和 SHA256 校验文件。默认版本 `0.1.0-dev`，可用 `-Version <version>` 指定；构建固定 Windows amd64 并关闭 CGO。

在 `dist` 中：

1. 复制 `clean-relay.example.json` 为 `clean-relay.json`，把 `upstream_origin` 改为上游的协议、主机和可选端口（不含路径）。
2. 校验并启动：

```powershell
.\clean-relay.exe -config .\clean-relay.json -check-config
.\clean-relay.exe -config .\clean-relay.json
```

3. 把客户端 Base URL 指向本机代理，API Key 与原来保持一致。

| 配置位置 | 示例 |
| --- | --- |
| 客户端原 Base URL | `https://upstream.example/v1` |
| 配置中的 `upstream_origin` | `https://upstream.example` |
| 客户端新 Base URL | `http://127.0.0.1:17880/v1` |

路径原样转发：`/v1/responses` 到达 `https://upstream.example/v1/responses`；原地址带 `/api/gateway/v1` 等前缀时，新 Base URL 保留完整前缀，`upstream_origin` 仍只填协议、主机和端口。

健康检查为 `GET http://127.0.0.1:17880/_cleanrelay/healthz`。Ctrl+C 停止接收新连接并等待最多 10 秒，第二次 Ctrl+C 立即退出；回滚时把客户端 Base URL 改回原地址即可。命令行参数、错误码和日志字段详见 [Windows 启动说明](docs/windows-usage.md)。

## 清理规则

清理只做一件事：遍历请求体顶层 `input` 数组，删除每个对象的直接属性 `internal_chat_message_metadata_passthrough`（字段列表由 `drop_input_fields` 配置）。数组长度与顺序、消息正文、工具调用、数字精度和其余字段保持不变；正文中出现同名字符串不会被删除，其他层级的同名字段不受影响。把 `drop_input_fields` 设为 `[]` 可关闭删除，仅保留 JSON 校验和大小限制。

清理对所有经过代理的 JSON 请求生效，不按上游最终选择的渠道区分——渠道是上游收到清理后的请求才决定的。修改配置后重启生效。

## 可选：Azure 资源归属错误修复

`azure_resource_retry` 默认关闭。启用后，只有 `.../responses` 或 `.../responses/compact` 请求精确命中 Azure 历史 item 资源归属错误、且响应尚未向客户端输出时，才按选定模式向同一上游地址发送修复副本；普通 HTTP 错误和未压缩 SSE 开头都支持有界检查，认证、限流、普通 400、网络错误和响应头超时不会触发。

| `cleanup_mode` | 修复顺序 | 上游调用上限 |
| --- | --- | --- |
| `portable_ids`（默认） | 清理可完整重放的消息/工具项的直接 ID，保留 reasoning | 2 次 |
| `portable_ids_and_reasoning` | 同一副本中清理 ID 并整项删除历史 reasoning | 2 次 |
| `portable_ids_then_reasoning` | 先清理 ID；再次精确命中且尚未输出时，再删除 reasoning | 3 次 |

逐级模式会跳过无变化的级别；所有副本都保留 compaction 和其他引用，删除 reasoning 属于有损降级。修改 `enabled` 或 `cleanup_mode` 后重启生效。

## 范围与限制

- 支持 HTTP JSON 请求（普通或单层 gzip）与 SSE 响应；无请求体的请求（如模型列表查询）原样转发。
- 认证信息沿用客户端请求头，配置文件不保存 API Key；直连上游并校验 TLS 证书，不读取系统或环境代理设置。
- 默认仅监听回环地址，请求体上限默认 32 MiB，并发上限默认 8；日志不记录认证头和请求、响应正文。
- 不支持 WebSocket、multipart/二进制上传和请求 trailers；GUI、托盘、自启动和通用规则引擎留待后续需求。

## 文档与样例

- [Windows 启动说明](docs/windows-usage.md)：配置、接入、日志字段、错误处理与退出；构建时复制为 `dist/README.md`。
- [配置样例](clean-relay.example.json)：完整配置，条件修复默认关闭。
- [输入样例](testdata/requests/metadata.before.json) / [期望输出](testdata/requests/metadata.after.json)：合成数据，展示 8 个元素中删除 7 个字段后的结果。

样例用于验证清理行为（数字精度、扩展字段），不作为发往渠道的业务请求。

## 开发

```powershell
gofmt -w cmd internal
go test ./...
go vet ./...
go test -race ./...   # Windows 需要 C 工具链
```

测试全部使用本地假上游，覆盖 JSON/gzip、数字精度与重复键、SSE 分段、取消、并发、超时以及条件修复的触发与限制；Windows 退出行为需在 Windows 上验证。行为变更请补充对应的回归测试。
