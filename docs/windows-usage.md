# CleanRelay Windows 启动与接入

CleanRelay 是本机 HTTP/SSE 请求清理代理，可选支持条件资源修复与逐级修复。

运行环境为 Windows 10 amd64；运行 exe 不需要安装 Go。交付目录包含 `clean-relay.exe`、`clean-relay.example.json`、本说明和 exe 的 SHA256 校验文件。

## 配置与启动

在交付目录中运行：

```powershell
Copy-Item .\clean-relay.example.json .\clean-relay.json
```

编辑 `clean-relay.json`，将 `upstream_origin` 改为实际上游服务的协议、主机和可选端口。配置不包含 API Key。API Key 继续由客户端提供。

```json
{
  "listen": "127.0.0.1:17880",
  "upstream_origin": "https://your-upstream-host.example",
  "drop_input_fields": ["internal_chat_message_metadata_passthrough"],
  "max_body_bytes": 33554432,
  "max_in_flight": 8,
  "response_header_timeout": "5m",
  "log_level": "info",
  "azure_resource_retry": {
    "enabled": false,
    "cleanup_mode": "portable_ids",
    "inspect_sse": true,
    "max_inspect_bytes": 65536,
    "inspect_timeout": "2s"
  }
}
```

上游只填 origin，可带末尾单个 `/`；不得包含路径前缀、用户名、密码、查询参数或片段。监听地址只能使用回环 IP 和非零端口，如 `127.0.0.1:17880` 或 `[::1]:17880`。

```powershell
.\clean-relay.exe -config .\clean-relay.json -check-config
.\clean-relay.exe -config .\clean-relay.json
```

`-check-config` 只检查配置，不监听、不联系上游。启动后终端显示监听地址和上游 origin。在另一个终端检查本地健康状态：

```powershell
Invoke-RestMethod http://127.0.0.1:17880/_cleanrelay/healthz
```

返回 `status: ok` 表示本地进程可服务。健康检查不验证上游网络、模型和 API Key。

## 客户端接入

将客户端 Base URL 的协议、主机和端口改为本机地址，保留原路径前缀和原 API Key。

| 原客户端 Base URL | `upstream_origin` | 新客户端 Base URL |
| --- | --- | --- |
| `https://your-upstream-host.example/v1` | `https://your-upstream-host.example` | `http://127.0.0.1:17880/v1` |
| `https://your-upstream-host.example/api/gateway/v1` | `https://your-upstream-host.example` | `http://127.0.0.1:17880/api/gateway/v1` |
| `http://127.0.0.1:3000/v1` | `http://127.0.0.1:3000` | `http://127.0.0.1:17880/v1` |

请求 `/v1/responses` 时，代理将完整路径及查询串转发到配置的上游。使用 HTTP JSON 或 SSE 接入；WebSocket、CONNECT、multipart/二进制上传和请求 trailers 不在首版范围内。

字段规则仅删除顶层 `input` 数组中对象的直接属性。正文、工具参数字符串和深层同名属性保留；数字精度保留。将 `drop_input_fields` 设为 `[]` 后停止发送前的元数据删除，JSON 校验和大小限制仍执行；条件修复由独立开关控制。修改配置后重启生效。

## 按资源归属错误修复

将 `azure_resource_retry.enabled` 改为 `true` 并重启即可启用；改回 `false` 并重启即可回退，无需修改客户端 Base URL。缺省该对象也默认关闭。

首次请求完成原有元数据清理后正常发送。只有 `POST .../responses` 或 `POST .../responses/compact` 命中精确的 Azure 历史 item 资源归属错误，且尚未向客户端输出，才按选定模式生成修复副本并顺序重发。每一级都要独立匹配错误，每个新请求重新判断。认证、限流、普通 400、网络错误和响应头超时不触发此功能。

| 选项 | 默认值与作用 |
| --- | --- |
| `cleanup_mode` | 默认 `portable_ids`；只能选择下表中的一个字符串值 |
| `inspect_sse` | `true`：检查 HTTP 200、未压缩的 SSE 开头；设为 `false` 后只检查 HTTP JSON 错误 |
| `max_inspect_bytes` | `65536`：原始错误体、解压错误体及 SSE 前缀各自的限额；允许 1 到 1048576 |
| `inspect_timeout` | `2s`：每次被检查的上游响应取得最终头后独立计时；必须大于零且不超过 `10s` |

`azure_resource_retry.cleanup_mode` 的三个可选值：

| 取值 | 命中错误后用于重发的清理动作 |
| --- | --- |
| `portable_ids`（默认） | 只删除内容完整、状态允许的消息/工具项的直接 `id`，保留所有历史 reasoning 项 |
| `portable_ids_and_reasoning` | 在同一份重发副本中完成上述 ID 清理，并整项删除顶层 `input` 中 `type` 为 `reasoning` 的历史项；文中的“强降级模式”指的就是这个值 |
| `portable_ids_then_reasoning` | 先尝试上述 ID 清理；第二次仍精确命中同类错误且尚未输出时，再发送同时清理 ID、删除 reasoning 的强修复副本 |

两个旧模式最多两次应用层上游调用，第二次响应直接返回；默认模式失败后不会自动升级。`portable_ids_and_reasoning` 在唯一一次重发前一起完成两类删除。显式选择 `portable_ids_then_reasoning` 才允许最多三次调用：首发、ID 修复、强修复；第三次结果直接返回。

逐级模式中，没有普通 ID 但存在 reasoning 候选时直接尝试强修复；没有 reasoning 候选时，ID 修复后的响应直接返回，不增加第二个检查窗口。没有候选、无新增删除或修复后 `input` 为空时不会发送该副本。各副本都从元数据清理后的首发快照生成，不修改客户端历史。删除 reasoning 会丢失历史推理状态，不承诺与完整历史效果等价。

修改模式后重启生效。升级不会自动改选逐级模式；回退到只支持旧模式的 exe 前，须将新增值改回 `portable_ids` 或 `portable_ids_and_reasoning`，仅关闭 `enabled` 仍会被旧解析器拒绝。

compaction、旧响应引用、`call_id` 和嵌套资源 ID 保留。旧 `drop_input_fields` 仍会无条件删除所列直接属性；需要保留历史时，应把 `id`、`type`、`content`、`encrypted_content` 等字段移出旧列表，推荐只保留兼容性元数据字段。

带 `Idempotency-Key`、`X-Idempotency-Key`、`Last-Event-ID` 或 `background: true` 的请求跳过条件修复。没有候选项时也直接透传，不增加 SSE 等待。gzip SSE、非 200 SSE 和输出已经开始的流直接交给客户端；上游 103 等信息性头部同样会关闭重试窗口。

候选请求的响应头和早期心跳可能延后最多配置的检查时间；首个文本、工具或其他输出事件会立即放行。超时后继续转发同一条流，不取消上游。逐级模式最多有两个独立检查窗口，默认检查预算合计最多 4 秒；命中后还需等待变换和后续调用，这不是整个请求的完成上限。每次调用独立沿用 `response_header_timeout`，共用客户端原 deadline，不为长流增加固定总时限。第二次一旦输出或结束检查，就不会再升级。

该功能无法控制上游最终选择的 Azure 资源，也不保证清理 ID 后密文或其他引用可跨资源使用。代理只匹配已知的精确错误形态，不提供通用规则引擎。

## 观察与停止

代理向 stderr 输出 `slog` 文本日志。每个请求记录本地 request ID、方法、路径、清理计数、状态、耗时和结果，不记录正文、认证头或查询参数。

```text
level=INFO msg=request request_id=local-... method=POST path=/v1/responses input_items=8 removed_fields=7 modified_items=7 status=200 duration_ms=8420 outcome=completed upstream_attempts=1 retry_cleanup_mode=portable_ids retry_decision=disabled
```

`outcome=canceled` 表示客户端或服务 context 取消；`outcome=interrupted` 表示响应转发中断。响应已开始后的失败会结束当前响应，不插入 JSON 或完成事件。

条件修复字段合并在同一条结束日志中：`upstream_attempts` 记录应用层实际调用次数（0 到 3），`retry_decision=retried` 一旦发起重发就保持不变；`retry_cleanup_mode` 始终记录配置策略。`retry_removed_item_ids` 和 `retry_removed_reasoning_items` 是最近一次成功生成副本相对首发快照的删除数，不跨级别累加，也不计入旧 `removed_fields`。`retry_rule=azure_resource_mismatch`、`retry_error_source=http_json` 或 `sse` 记录首次匹配，`first_upstream_status` 记录首次响应状态。

候选请求另有 `retry_stop_reason`，说明最终为何停止修复，如 `no_match`、`output_started`、`inspection_timeout`、`canceled` 或 `stages_exhausted`；最后一个只表示计划已耗尽，不代表模型成功。实际发送第 2/3 次调用时才分别创建 `retry_attempt_2` / `retry_attempt_3` 分组，包含本次清理级别、紧邻触发响应的状态/来源，以及相对首发的删除数。生成后因空数组或取消而未发送的副本没有对应分组。

当前不会在检测到错误或开始重发的瞬间单独打印日志，以上字段要等整个请求处理结束后才输出。对于 SSE，需要等流结束、中断或取消。正常传输完成的结束日志为 `INFO` 级别，默认 `log_level: "info"` 可见；设置为 `warn` 或 `error` 会过滤此类记录。

`retry_business_failure=true` 保留任何检查窗口内曾识别到的业务失败；最终返回的响应本身已被识别为业务失败时，才使用 `outcome=upstream_business_error`。`outcome=completed` 仅表示传输完成；未检查的最终响应、HTTP 200、调用次数或 SSE 正常 EOF 都不能证明模型请求成功。日志不包含错误正文、资源 ID、密文或密钥。

在运行终端按 Ctrl+C：停止接受新连接，最多等待 10 秒让已有请求完成，再关闭长流和上游连接。第二次 Ctrl+C 会立即关闭。程序结束后监听端口释放。

绕过代理时，将客户端 Base URL 恢复为原上游地址，继续使用原 API Key。

## 参数和错误

| 参数或退出码 | 含义 |
| --- | --- |
| 无参数 | 读取当前工作目录中的 `clean-relay.json` |
| `-config <path>` | 指定配置；相对路径按当前工作目录解析 |
| `-check-config` | 离线检查配置后退出 |
| `-version` | 打印构建版本，不读取配置 |
| `-help` / `-h` | 打印用法 |
| 退出码 `0` | 正常停止、检查成功或信息查询成功 |
| 退出码 `2` | 参数错误、配置无法读取或配置无效 |
| 退出码 `1` | 监听或运行失败 |

配置错误会指出相关字段或约束；端口冲突会显示监听地址，可修改 `listen` 后重启。未知字段、重复键、显式 `null` 和错误类型都会使配置检查失败。

| 本地 HTTP 状态 | 处理方式 |
| --- | --- |
| `400` | 检查 JSON、重复对象键或 gzip 完整性 |
| `408` | 检查客户端请求体是否及时发送完成 |
| `413` | 缩小请求，或按资源情况调整 `max_body_bytes` |
| `415` | 使用 JSON Content-Type 和单层 gzip/identity 编码 |
| `501` | 改用首版支持的 HTTP JSON/SSE 接入方式 |
| `502` | 检查上游 origin、网络与 TLS 证书 |
| `503` | 已达到 `max_in_flight`，`Retry-After: 1` |
| `504` | 检查上游连接或调大/关闭响应头等待限制 |

上游返回的 `401`、`429`、`5xx` 和重定向状态及正文会直接交还客户端。代理只支持上文显式启用的精确资源归属修复，不进行通用状态码重试，也不跟随重定向。上游采用直连并校验证书；不读取系统或环境 HTTP 代理配置。

接入后建议完成一次普通对话、一次工具调用和一次取消，并用日志中的清理计数或上游观察确认目标字段已删除。
