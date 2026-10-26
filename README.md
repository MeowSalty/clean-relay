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

基础清理遍历请求体顶层 `input` 数组，删除每个对象的直接属性 `internal_chat_message_metadata_passthrough`（字段列表由 `drop_input_fields` 配置）。数组长度与顺序、消息正文、工具调用、数字精度和其余字段保持不变；正文中出现同名字符串不会被删除，其他层级的同名字段不受影响。把 `drop_input_fields` 设为 `[]` 可关闭删除，仅保留 JSON 校验和大小限制。

清理对所有经过代理的 JSON 请求生效，不按上游最终选择的渠道区分——渠道是上游收到清理后的请求才决定的。修改配置后重启生效。

## 可选：Azure 资源归属错误修复

`azure_resource_retry` 默认关闭。启用后，只有 `.../responses` 或 `.../responses/compact` 请求精确命中 Azure 历史 item 资源归属错误、且响应尚未向客户端输出时，才按选定模式向同一上游地址发送修复副本；普通 HTTP 错误和未压缩 SSE 开头都支持有界检查，认证、限流、普通 400、网络错误和响应头超时不会触发。

| `cleanup_mode` | 修复顺序 | 上游调用上限 |
| --- | --- | --- |
| `portable_ids`（默认） | 清理可完整重放的消息/工具项的直接 ID，保留 reasoning | 2 次 |
| `portable_ids_and_reasoning` | 同一副本中清理 ID 并整项删除历史 reasoning | 2 次 |
| `portable_ids_then_reasoning` | 先清理 ID；再次精确命中且尚未输出时，再删除 reasoning | 3 次 |

逐级模式会跳过无变化的级别；所有副本都保留 compaction 和其他引用，删除 reasoning 属于有损降级。修改 `enabled` 或 `cleanup_mode` 后重启生效。

### 会话范围修复

`history_scope` 默认是 `request`，保持每次请求独立修复。显式选择 `session` 并配置经过验证的 `session_header` 后，首发只对匹配的旧项重放已成功写出的清理动作。每次再次精确命中资源错误，都可加入本次失败请求中仍可清理的项；修复副本必须保留本请求已采用的动作，并至少新增一个动作。三个清理挡位均支持这一范围，上游调用上限仍为 2/2/3 次；目标 Azure 渠道可选 `portable_ids_and_reasoning`。

会话头由接入方提供，程序没有内置已验证的 Codex 桌面头名。可先开启 `session_diagnostics`，通过允许列表中头的存在性与短 HMAC 验证同一聊天跨轮稳定、不同聊天独立、压缩前后可关联。缺少有效身份时记录 `session_identity_unavailable`，仅执行基础清理和转发。

缓存只保存不可逆指纹、动作和计数，默认空闲 2 小时过期、最多 256 个会话、每会话 4096 个选择器及锚点、总预算 64 MiB。候选可先保存，只有成功写回调确认的动作才供后续首发使用。重启、过期或整组淘汰会丢失记忆；并发请求在同一存活期内合并动作，范围扩展只增加修订号。完成判定要求明确的 JSON/SSE 完成状态，HTTP 200 或 EOF 不足以确认恢复。配置示例、匹配限制和日志解释见 [会话接入说明](docs/windows-usage.md#会话范围修复)。

本次版本改变了 `session` 的恢复语义：上一轮取消、断流、失败或没有 `completed`，均不阻止下一次精确错误触发新增清理。完成日志仅用于诊断，既有配置自动采用新规则；切回 `request` 只改变历史记忆方式，恢复旧 completed 门槛需要回退程序版本。成功、普通错误或仅追加历史不会扩大范围，但持续资源错误可能使新 reasoning 陆续被删除。匿名项无法跨请求对齐时，仍需本次错误重新授权；未支持的引用仍可能导致修复失败。通知压缩和自动压缩仍属后续路线。

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

测试全部使用本地假上游，覆盖 JSON/gzip、数字精度与重复键、SSE 分段、取消、并发、超时，以及会话范围刷新、缓存存活期、修订合并、内存淘汰和完成观察；Windows 退出行为需在 Windows 上验证。既有桌面会话头与渠道切换的人工接入验收不替代本次新增流程：“修复写出后取消 → 新增历史 → 同类错误 → 再次恢复”的真实渠道验收仍待完成。行为变更请补充对应的回归测试。
