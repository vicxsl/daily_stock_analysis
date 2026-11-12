# 个人补丁维护

本分支从上游 v3.33.0（`bba8bf51bdbd3badc75a832c52ede058ef5ab871`）建立，保存 2026-11-07 NAS 生产镜像使用的个人源码补丁。补丁涵盖账户基线及写入保护、基金身份与净值路由、价格质量、告警与风险、信号质量、回测重试、调度收尾，以及对应测试。`docker/Dockerfile` 添加 `procps`。

2026-11-08 核对 NAS 19 个运行源码文件与本分支内容一致（按 LF 换行规范化）；生产镜像与运行配置、数据仍以 NAS 为准。生产 compose 的 `init: true` 和重建保护不在此源码补丁中。该分支本身不代表已部署或已验证上游更新。

`main` 跟随上游代码并保存个人自动化工作流，个人业务变更在此分支维护。不得把生产 `.env`、凭据、数据库、账户记录或其他私有配置加入仓库。

个人分支的 push 会触发 `Personal NAS deployment`，经受限 SSH 请求 NAS 拉取分支最新提交、从 `docker/Dockerfile` 构建、运行断网个人契约测试，再执行备份和受保护部署。工作流支持手动重试，部署失败会返回失败状态。构建日志、测试结果、提交 SHA、镜像、备份和恢复结果保留在 NAS 的 `dsa-update/state/runs/`。同一提交已经成功上线时验证现有镜像并跳过重建。

`Personal upstream sync PR` 每天 04:30（Asia/Taipei）检查上游 main，更新 `codex/upstream-sync` 并创建或更新面向个人分支的 PR，也支持手动执行。冲突在 PR 展示，由用户检查并合并。合并后触发上述部署流程，上游提交、PR 合并提交和部署 SHA 可用于追溯。专用同步分支由任务管理，勿在该分支保存个人改动。

工作流连接参数使用仓库变量 `DSA_DEPLOY_BRANCH`、`DSA_DEPLOY_HOST`、`DSA_DEPLOY_USER`、`DSA_DEPLOY_HOST_KEY`、`DSA_UPSTREAM_REPOSITORY`。部署私钥使用 secret `DSA_DEPLOY_SSH_KEY`，两跳 SSH 均限定为部署请求并校验主机指纹。NAS 通过公开 HTTPS 拉取源码。上游同步通过 secret `DSA_UPSTREAM_SYNC_SSH_KEY` 中的本仓库写入 deploy key 推送上游提交，以支持其中的工作流文件更新；PR 操作使用 Actions 的 pull-requests 写权限。

此前部署验证和恢复入口见本机投研运维仓库 `ai-investment/patches/dsa/v3.33.0/README.md`；生产重建使用既有 `dsa-redeploy.sh wrap`，不要直接重建触发额外研究。热点列表以最多两个板块并发取成分股，局部数据源异常向页面提示；生产实测和完整链路验收以实际部署结果为准。

本地验证遵循个人补丁的行情时间契约：来源时间未知或过期的价格告警记为降级，不发送通知；正常触发测试必须提供来源时间。同一来源时点的数据库规则触发记录会去重，冷却与通知重试仍分别验证。回测空持仓路径保留 `retry_incomplete=True`。静态 OpenAPI 同步个人基线、证券身份、事件类型、估值引用及可空现金/权益；Web 类型与可空值保持一致，现有金额显示器将未知值显示为 `--`。

热点详情的共享截止时间使用可控时钟回归，不依赖 Windows 的毫秒休眠精度；AkShare 契约测试在 Requests 会话入口拦截 HTTP，兼容旧版直调与新版统一 HTTP 入口，保留真实参数构造。上述离线验证不代表生产热点刷新已恢复。POSIX 进程、时区与权限测试需要 Linux 复验；本地 Windows 结果及未完成项见忽略目录 `local/validation-report.md`。

2026-11-08 Windows 离线完整回归为 8328 通过、124 失败、6 跳过，4 项联网测试未运行；其中迁移记录预期随后修正，迁移与基线补充复验 17 项通过，未再次执行完整回归。其余 123 项中，121 项在未打补丁的 v3.33.0 也失败，另 2 项新增调度测试缺少 POSIX 接口。19 个个人补丁及本轮修正涉及的测试文件在完整回归中为 527 通过、3 失败、3 跳过。Web 构建、lint（0 错误、1 条既有警告）和组合相关 97 项测试通过。NAS SSH 连接超时，Linux 验证尚未完成；本轮未提交、推送或部署。
