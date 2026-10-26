# CleanRelay Windows 启动与接入

CleanRelay 是本机 HTTP/SSE 请求清理代理，可选支持条件资源修复、逐级修复和会话范围修复。

运行环境为 Windows 10/11 x64（amd64）；运行 exe 不需要安装 Go。交付目录包含 `clean-relay.exe`、`clean-relay.example.json`、本说明和 exe 的 SHA256 校验文件。

## 下载与校验

从 [GitHub Releases](https://github.com/MeowSalty/clean-relay/releases) 下载 `clean-relay.exe`、`clean-relay.exe.sha256`、`clean-relay.example.json` 和 `README.md`，放入同一目录。在 PowerShell 中核对二进制校验值并查看版本：

```powershell
$expectedHash = (Get-Content -LiteralPath .\clean-relay.exe.sha256).Split(' ')[0]
$actualHash = (Get-FileHash -LiteralPath .\clean-relay.exe -Algorithm SHA256).Hash
if ($actualHash -ine $expectedHash) { throw 'SHA256 校验失败，请重新下载。' }
.\clean-relay.exe -version
```

升级时先退出旧进程，保留自己的 `clean-relay.json`，替换 exe 并重新校验配置后启动。示例配置用于对照新增选项，不要覆盖已有的本地配置。

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
    "history_scope": "request",
    "session_diagnostics": false,
    "session_cache": {
      "idle_ttl": "2h",
      "max_sessions": 256,
      "max_items_per_session": 4096,
      "max_bytes": 67108864
    },
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

默认 `history_scope=request`：首次请求完成原有元数据清理后正常发送。只有 `POST .../responses` 或 `POST .../responses/compact` 命中精确的 Azure 历史 item 资源归属错误，且尚未向客户端输出，才按选定模式生成修复副本并顺序重发。每一级都要独立匹配错误，每个新请求重新判断。认证、限流、普通 400、网络错误和响应头超时不触发此功能。

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

## 会话范围修复

`history_scope=session` 会合并每次精确资源错误对应请求中的候选项。后续首发先完成元数据清理，再仅对精确匹配的旧项重放已成功写出的动作；新消息和 reasoning 在进入自己的失败请求之前保留。范围更新不要求上一轮 completed，客户端保存的历史不被改写。

### 验证会话标识

程序没有默认会话头名。`session_id`、`conversation_id` 等是诊断候选，不代表所有 Codex 版本都提供稳定的会话协议。

1. 保持 `history_scope=request`，设置 `session_diagnostics=true`；可把准备验证的头名填入 `session_header`。诊断在 `enabled=false` 时也可单独使用。
2. 重启后，在同一聊天发送多轮请求，再使用另一个聊天并检查日志中的短 HMAC；同一聊天应稳定，不同聊天应不同。手动压缩前后也需能关联。
3. 确认后，设置 `history_scope=session`、`enabled=true`，并将 `session_header` 固定为已验证的头名，再重启。

诊断仅允许 `session_id`、`conversation_id`、`x-cleanrelay-session-id` 和显式配置的头，记录是否存在、是否合法和 16 个十六进制字符的 HMAC，不输出原始值或凭据。短 HMAC 只用于本进程内比对，重启后会变化；诊断不会自动宣布某个头已可信。

头名必须为不超过 128 字节的 HTTP token，不能选认证头、User-Agent、请求 ID 或已知安装/回合标识头。运行时会话值要求单一、非空、不超过 1024 字节的可见 ASCII，不含空白或逗号。缺失、重复、无效值，以及多值凭据或非法模型，都会记录 `session_identity_unavailable` 并仅执行基础清理和转发，不自动回退到逐请求范围修复。

### 配置示例与预算

下面是可替换现有 `azure_resource_retry` 对象的示例。`X-CleanRelay-Session-ID` 是客户端集成示例：客户端需要为每个逻辑会话发送稳定且独立的值；使用桌面版已有头时，应替换为实测验证后的名称。

```json
{
  "enabled": true,
  "cleanup_mode": "portable_ids_and_reasoning",
  "history_scope": "session",
  "session_header": "X-CleanRelay-Session-ID",
  "session_diagnostics": false,
  "session_cache": {
    "idle_ttl": "2h",
    "max_sessions": 256,
    "max_items_per_session": 4096,
    "max_bytes": 67108864
  },
  "inspect_sse": true,
  "max_inspect_bytes": 65536,
  "inspect_timeout": "2s"
}
```

| 选项 | 默认值与约束 |
| --- | --- |
| `history_scope` | `request`；只接受 `request` 或 `session` |
| `session_header` | 无默认值；`session` 模式必须显式提供 |
| `session_diagnostics` | `false`；布尔值，可独立启用 |
| `session_cache.idle_ttl` | `2h`；正 Go 时长，匹配旧项刷新空闲时间 |
| `session_cache.max_sessions` | `256`；正整数，会话按凭据身份隔离 |
| `session_cache.max_items_per_session` | `4096`；正整数，合计各路径、模型命名空间中的选择器及锚点，含未写出的候选 |
| `session_cache.max_bytes` | `67108864`；正整数，缓存结构采用保守字节计费 |

超限按会话整组淘汰最久未使用的记录，单个超大组不部分保留。未写出的候选同样计入预算；超大请求仍可局部修复，但成功写回调不会偷偷重建缓存。该预算限制持久于请求之间的缓存；正在转发的请求及完成观察缓冲另受请求大小、并发数和观察上限约束。重启、过期或淘汰会丢失记忆；下一次有效请求精确命中资源错误时，以它自己的快照重新建立范围。

未知键、重复键、`null`、非法类型和预算越界继续严格拒绝，即使 `enabled=false` 也会校验。回到逐请求范围时设置 `history_scope=request`；回退到不支持会话配置的旧 exe 前，需移除以上新增字段。

### 匹配与恢复状态

会话范围按客户端凭据、会话头、上游 origin、路径前缀、模型和查询串隔离；同一路径前缀的 `/responses` 与 `/responses/compact` 共用范围。不同会话之间不共享边界，分支若采用不同会话标识也独立处理。

带 ID 的项需要类型、ID 及内容指纹一致；无 ID 的 reasoning 可使用密文指纹。匿名 reasoning 仅在双侧唯一锚点、区间内容及出现次数一致时跨请求匹配。首发无法对齐的匿名项保留并记录 `scope_alignment_unavailable`；若本次又精确报错，则可使用本次快照的请求局部选择器清理，不能把匿名指纹变成全会话删除规则。编辑、裁剪或重复插入可能让旧项无法匹配；手动压缩后的新窗口若不再含旧选择器，自然不产生预清理，回退后重新出现的可识别旧项仍会匹配。短历史不能覆盖并清空此前记忆。

三个挡位仅作用于选中范围。逐级模式记住实际发送过的升级动作；每个入站请求的上游调用上限仍为两次或三次。空数组、无变化副本、幂等键、流恢复、后台请求、取消和输出后的重试边界同样适用于会话模式的首发预处理。

候选冻结、动作写出和恢复完成分别记录。精确错误可以先保存未应用候选，只有该次上游调用收到成功 `WroteRequest` 回调，实际采用的动作才供后续首发使用。连接失败、失败写回调或发送前取消不能算作已应用；成功写出后的取消、断流、客户端写入失败或业务错误不会撤销这些动作。

每次符合条件的请求再次精确命中资源错误，都能合并本请求中仍可清理的候选，不要求上一轮 completed。首发候选数按实际输入项减去预清理删除数计算；修复副本全部来自同一份元数据清理后的快照，保留本请求上次发送的全部动作，并至少新增一个动作。没有剩余候选时记录 `no_candidate`，拟发送副本没有新增动作时记录 `no_change`，均不原样重发。符合清理资格的当前用户消息也可能删除直接 ID，正文仍保留；普通项自身 `status` 缺省或为 `completed` 的资格规则保持不变。

例如 A 修复写出后取消，下一次 A+B 首发只清理 A；该请求再次精确报错时可清理 B 并重发。随后 A+B+C 正常成功时只清理 A、B，C 保留；C 只有进入自己的精确失败请求后才可能增加清理。成功、普通错误、单纯追加历史或输出后的 SSE 错误均不扩展范围。

同一缓存存活期的候选和已写出动作只增长。P 携带 A+B、Q 携带 A+D 时，各自修复仍只处理自己的请求，写回调无论先后都会合并成 A+B+D；较旧修订的回调不能误标后来追加的条目。淘汰或过期后的旧请求和旧回调不能复活记录，即使同一键已经建立新记录；在途请求记录 `scope_changed` 并交还当前响应，下次请求重新判断。

完成观察只查看原样转发的字节，不额外读流或等待完整长流。JSON 要求完整文档、`status=completed`、数组形态的 `output`、无错误和未完成信息；若提供 `object`，必须为 `response`。SSE 要求一致的 `response.completed` 事件及嵌套完成状态。单个 JSON 文档或 SSE 事件上限为 1 MiB，SSE 总长度不受该上限限制。`inspect_sse` 仅控制早期错误检查，完成观察独立进行。

HTTP 200、`[DONE]` 或 EOF 单独都不构成成功证据。失败、超限、压缩响应、不支持的格式、取消、断流或客户端写入失败不能确认 `recovery_acceptance=completed`，转发字节不变。`recovery_observation` 只描述本响应的解析结果；旧并发响应即使被观察为 completed，也不能确认已扩大的新修订。两种日志都不控制下一次候选生成、范围扩展或重发。

本次版本直接更新现有 `session` 配置的行为，不增加开关。持续资源错误可能使新增 reasoning 陆续进入删除范围；程序仍不能定位唯一坏项、修复未支持的引用或控制 AxonHub 路由。回到 `request` 仅改变历史记忆方式；若必须恢复“上一轮 completed 后才允许扩大范围”的旧策略，需要回退程序版本。

### 会话日志

结束日志增加以下字段，原有元数据与重试计数保留：

| 字段 | 含义 |
| --- | --- |
| `history_scope` | 会话模式为 `session` |
| `scope_cache` | `miss`、`hit`、`scope_created`、`scope_reused`、`scope_extended`、`scope_cache_budget` 或 `scope_changed` |
| `scope_generation` / `scope_revision` | 缓存记录的存活期及修订；追加范围只增加修订，不能推断资源切换次数 |
| `scope_pre_removed_item_ids` / `scope_pre_removed_reasoning_items` | 本次首发预处理的实际删除数 |
| `scope_added_selectors` | 本次错误实际加入缓存的去重候选数；未缓存时为 0，不表示动作已写出 |
| `scope_applied_selectors` | 所观察缓存版本中已应用的选择器数量，非本次删除数 |
| `scope_write_confirmed` | 最后一次带恢复范围的上游请求是否收到成功写入回调 |
| `scope_alignment` | 无法安全对齐匿名项时为 `scope_alignment_unavailable` |
| `recovery_observation` | 本次响应的完成观察结果，如 `completed`、`business_failure`、`completion_limit` |
| `recovery_acceptance` | 仅正常转发结束且成功确认对应缓存版本时为 `completed`，其他为 `unknown` |

`scope_created` / `scope_extended` 只表示保存了候选；`scope_write_confirmed=true` 只表示请求已写出，均不等于上游接受或业务恢复。过时响应即使观察到完成事件，也不能把 `recovery_acceptance` 记为完成。通知压缩、自动压缩和 `recovery_action` 配置尚未实现。

本次新增流程的真实渠道验收仍待完成：让强修复请求成功写出后取消，追加历史并再次触发同类资源错误，确认首发只预清理旧项、随后只增加本请求的动作且总调用不超过两次。记录清理数量、调用次数及完成诊断即可，不记录正文或凭据。本地假上游自动测试不替代这一接入验收。

## 观察与停止

代理向 stderr 输出 `slog` 文本日志。每个请求记录本地 request ID、方法、路径、清理计数、状态、耗时和结果，不记录正文、认证头或查询参数。

```text
level=INFO msg=request request_id=local-... method=POST path=/v1/responses input_items=8 removed_fields=7 modified_items=7 status=200 duration_ms=8420 outcome=completed upstream_attempts=1 retry_cleanup_mode=portable_ids retry_decision=disabled
```

`outcome=canceled` 表示客户端或服务 context 取消；`outcome=interrupted` 表示响应转发中断。响应已开始后的失败会结束当前响应，不插入 JSON 或完成事件。

条件修复字段合并在同一条结束日志中：`upstream_attempts` 记录应用层实际调用次数（0 到 3），`retry_decision=retried` 一旦发起重发就保持不变；`retry_cleanup_mode` 始终记录配置策略。`retry_removed_item_ids` 和 `retry_removed_reasoning_items` 是最近一次成功生成副本相对元数据清理后、会话预清理前的 `B0` 的总删除数，不跨级别累加，也不计入旧 `removed_fields`。`retry_rule=azure_resource_mismatch`、`retry_error_source=http_json` 或 `sse` 记录首次匹配，`first_upstream_status` 记录首次响应状态。

候选请求另有 `retry_stop_reason`，说明最终为何停止修复，如 `no_match`、`output_started`、`inspection_timeout`、`canceled` 或 `stages_exhausted`；最后一个只表示计划已耗尽，不代表模型成功。实际调用第 2/3 次上游时才分别创建 `retry_attempt_2` / `retry_attempt_3` 分组，包含本次清理级别、紧邻触发响应的状态/来源，以及相对 `B0` 的总删除数。新增的 `new_removed_item_ids` / `new_removed_reasoning_items` 表示比本请求上一份已发送副本多删除的数量，不以最新缓存作为差量基准。生成后因空数组或取消而未调用的副本没有对应分组；调用过不等于收到成功写回调。

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
