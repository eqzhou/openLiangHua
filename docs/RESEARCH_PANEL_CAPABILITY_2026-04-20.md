# Research Panel Capability

CAPABILITY
- 面向量化研究与驾驶舱运行时，我们需要把原先分散在 `feature_panel`、`label_panel` 以及若干 artifact/blob 中的研究截面，统一收敛为数据库中的 `research.panel` 宽表，并配套 `research.panel_runs` 记录每次重建批次。新的能力完成后，系统的研究链路将从“基础全市场日线主表 `market.bars_1d` -> `research.panel` -> 模型训练/最新推理/候选池/页面读取”运行，避免继续依赖本地 parquet 或超大 artifact blob。

CONSTRAINTS
- `market.bars_1d` 是全市场日线基础真源，`research.panel` 只承接研究加工结果，不回写原始行情。
- `research.panel` 必须按 `(data_source, trade_date, ts_code)` 唯一，保证同一来源同一交易日同一股票只有一行。
- `research.panel` 要覆盖当前特征列和标签列，允许最新未标注交易日的标签为空。
- 运行时要支持 `tushare`、`akshare`、`myquant` 多来源，不把数据源硬编码死在表结构之外。
- 批次写入必须可观察，至少要记录 `run_id`、状态、起止日期、行数、股票数、失败信息。
- 读取层要兼容当前主工程和测试目录：主工程读数据库真源，测试目录可继续用临时样本，不阻断回归。
- 新表不是万能大杂烩，不承载模型预测、组合指标、LLM 原文等其他域数据。

IMPLEMENTATION CONTRACT
- Actors
  - 数据工程任务：根据全市场日线主表生成并写入 `research.panel`
  - 研究任务：从 `research.panel` 读取训练/推理输入
  - 运维与页面：通过 `research.panel_runs` 观测最近一次重建状态
- Surfaces
  - 数据库 schema：`research.panel`、`research.panel_runs`
  - 仓储接口：`save_research_panel`、`load_research_panel`、`load_research_panel_summary`
  - 特征构建任务：把 feature/label 合并后写入 `research.panel`
  - 模型工作区：优先从 `research.panel` 读取，不再依赖 feature/label blob
- States and transitions
  - `panel_runs.status` 至少包含 `running`、`succeeded`、`failed`
  - 新批次开始时写入 `running`
  - 数据批量写入成功后更新为 `succeeded`
  - 任一阶段异常时更新为 `failed` 并记录错误消息
- Interface/data implications
  - `research.panel`
    - 主键：`(data_source, trade_date, ts_code)`
    - 核心元数据：`name`、`industry`、`index_code`、`is_current_name_st`、`is_index_member`、`days_since_list`
    - 当前特征列：`pct_chg`、`ret_1d`、`mom_5`、`mom_20`、`mom_60`、`mom_120`、`vol_20`、`close_to_ma_20`、`vol_60`、`close_to_ma_60`、`amount_20`、`downside_vol_20`、`ret_skew_20`、`drawdown_60`
    - 当前标签列：`can_enter_next_day`、`ret_next_1d`、`label_valid_t5`、`ret_t1_t5`、`label_valid_t10`、`ret_t1_t10`、`label_valid_t20`、`ret_t1_t20`
    - 批次关联：`run_id`
  - `research.panel_runs`
    - `run_id`、`data_source`、`status`、`date_min`、`date_max`、`row_count`、`symbol_count`
    - `feature_columns`、`label_columns` 使用 JSONB 记录列清单
    - `message` 记录失败信息或摘要
- Observability and operator requirements
  - 可以按 `data_source` 查看最近一次成功批次
  - 可以按 `trade_date`、`ts_code` 查询 panel 行
  - 页面和脚本摘要不再回传本地文件路径，只返回 `run_id` / artifact 引用 /统计元数据

NON-GOALS
- 不在本次内把模型预测、回测组合、LLM 响应也塞进 `research.panel`
- 不重写 `market.bars_1d` 的行情落库逻辑
- 不在本次内完成所有下游读取迁移；先落表和写入链路，再逐步切读链路

OPEN QUESTIONS
- `research.panel` 是否需要额外保存 `asset_type`、`exchange` 等字段，当前先依赖 `ts_code` 和上游 `ref.instruments`。
- 后续是否需要为不同特征集版本加 `panel_version`；当前先按 `(data_source, trade_date, ts_code)` 覆盖式更新。
- 全市场重建的批量写入是先走 `COPY` 到临时表再 merge，还是先用 chunked upsert；本次优先实现安全可落地的批量写入。

HANDOFF
- 已具备直接实现条件。
- 下一步由 `tdd-workflow` 落测试，再由仓储/数据库实现 `research.panel` 与 `research.panel_runs`，随后把特征构建写入链路切过去。
