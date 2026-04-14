# 前端与应用层重构提案

日期：2026-04-02

## 1. 先说结论

这个项目当前最大的结构问题，不是量化研究层完全失控，而是“应用层缺位”。

现状里：

- `src/data`、`src/features`、`src/models`、`src/backtest` 这几层已经有初步职责分工。
- 但 `streamlit_app.py` 同时承担了数据读取、配置读取、报表装配、任务编排、页面状态、表格展示和交互逻辑。
- 结果就是前端文件越来越像“总控脚本”，而不是展示层。

我的建议是：

1. 短期不要直接重写成 React + FastAPI。
2. 先保留 Streamlit 作为壳。
3. 先把应用服务层补出来，把页面逻辑从 `streamlit_app.py` 下沉。
4. 等应用层稳定后，再决定是否拆真正的前后端。

这是当前项目最稳、最省成本、也最容易持续推进的路线。

## 2. 现状诊断

### 2.1 当前结构并不是“完全没分层”

当前仓库已经有这些相对清晰的技术分区：

- `src/data`: 数据下载、面板落库、日历和成分股处理
- `src/features`: 因子和标签构建
- `src/models`: 训练、walk-forward、推理、评估
- `src/backtest`: 组合构建、风控、指标
- `src/agents`: AI 叠加、盯盘清单、操作备忘、LLM bridge
- `src/utils`: IO、日志、价格标记、讨论回写等

问题不在“没有模块目录”，而在“缺应用层”。

### 2.2 主要结构问题

#### 问题 A：前端文件过重

当前 `streamlit_app.py` 约 2017 行，47 个函数，明显承担了过多职责。

其中有几个典型信号：

- `_build_watchlist_view` 约 186 行，已经不是简单展示转换，而是持仓领域的核心装配逻辑。
- `_render_overlay_panel` 和 `_render_overlay_panel_v2` 分别约 124 / 161 行，页面渲染逻辑过重且存在重复。
- 大量 `_load_*` 函数直接在前端读取 `yaml/json/parquet/csv/md`。
- `_run_module` 直接在页面里发起子模块执行，页面层和任务编排层耦合。

换句话说，前端已经不是 View，而是半个 Application Service。

#### 问题 B：领域逻辑散落在多个脚本里

几个关键领域都存在“共享逻辑没有抽出来”的问题：

- `src/agents/action_memo.py` 直接 import `src/agents/watch_plan.py` 里的私有 helper
- `watch_plan.py` 和 `action_memo.py` 共享同一份持仓快照逻辑，但没有独立的 holding domain/service
- `overlay_report.py` 既做候选装配，也做中文研判、报表落盘、LLM 请求导出

这说明当前不是缺函数，而是缺稳定的领域边界。

#### 问题 C：原始 DataFrame 直接成为页面契约

当前很多页面和服务之间传递的是“带几十列字段的 DataFrame”，而不是稳定的 view model / DTO。

后果是：

- 字段命名变动会波及整个页面
- UI 很难只依赖自己真正需要的数据
- 逻辑复用只能靠复制列名和拼装规则
- 单测很难只测某个页面场景

#### 问题 D：页面分区已经出现“复制型扩张”

比如：

- overlay 有两个渲染函数版本
- watchlist 页同时维护总览、筛选、明细、AI 回写、盯盘清单、操作备忘、减仓计划
- service 页同时处理状态、日志、说明

继续往里加功能，会越来越难维护。

#### 问题 E：测试还没有覆盖应用层和展示层

目前测试主要集中在：

- `test_watch_plan.py`
- `test_action_memo.py`
- `test_latest_inference.py`
- `test_llm_bridge.py`
- `test_llm_discussion.py`
- `test_myquant_panel.py`
- `test_prediction_snapshot.py`

这说明“算法和工具函数”已有一定保护，但“页面装配层”和“应用服务层”几乎没有测试护栏。

## 3. 我建议的目标架构

### 3.1 近期目标：先做“分层单体”

短期最合理的是保留单仓库、保留 Streamlit，但改成下面这种结构：

```text
Presentation (Streamlit 页面/组件)
  -> Application (服务编排 / ViewModel 装配 / 任务触发)
    -> Domain (持仓 / 研判 / 信号 / 组合规则)
      -> Infrastructure (Parquet/YAML/CSV/LLM/外部数据接口)
```

也就是：

- 页面只负责交互和展示
- 应用层负责“这页需要什么数据、怎么拼”
- 领域层负责“持仓、候选、AI 研判、盘前建议”这些业务规则
- 基础设施层负责读写文件、调用 OpenAI、访问 MyQuant/AKShare

### 3.2 中期目标：让 Streamlit 可以被替换

当应用层和领域层抽稳以后：

- Streamlit 仍然能继续用
- 但未来如果要换成 Web 前端，不需要重写研究引擎
- 只要新增一个 API adapter 即可

这才是一个“可演进”的架构。

## 4. 建议的新目录

我建议先新增 `src/app`，而不是马上大规模重命名当前目录。

```text
src/
  app/
    actions/
      pipeline_runner.py
    repositories/
      config_repository.py
      report_repository.py
      market_repository.py
      note_repository.py
    services/
      dashboard_service.py
      watchlist_service.py
      overlay_service.py
      holding_note_service.py
      pipeline_service.py
      diagnostics_service.py
    viewmodels/
      overview_vm.py
      watchlist_vm.py
      overlay_vm.py
      service_status_vm.py
    pages/
      overview_page.py
      factors_page.py
      backtest_page.py
      candidates_page.py
      watchlist_page.py
      ai_page.py
      service_page.py
    components/
      metrics.py
      tables.py
      forms.py
      status_panels.py
```

然后让 `streamlit_app.py` 缩成一个入口文件，只做：

- 页面初始化
- session state 初始化
- 页面装配
- tab 路由

## 5. 当前模块到目标模块的映射

### 5.1 `streamlit_app.py`

当前职责：

- 文件读取
- 配置读取
- 报表读取
- 持仓装配
- 任务执行
- 页面渲染
- 服务状态展示

目标拆法：

- `src/app/repositories/*`: `_load_*` 读取函数
- `src/app/services/watchlist_service.py`: `_build_watchlist_view`、筛选、排序
- `src/app/services/overlay_service.py`: overlay 页面装配
- `src/app/actions/pipeline_runner.py`: `_run_module`
- `src/app/pages/*`: 每个 tab 一页
- `src/app/components/*`: 常用 metric/table/form

### 5.2 `src/agents/watch_plan.py` 与 `src/agents/action_memo.py`

当前问题：

- `action_memo.py` 依赖 `watch_plan.py` 的内部实现
- 持仓快照逻辑没有独立出口

目标拆法：

- 提取 `src/app/services/holding_snapshot_service.py`
- `watch_plan.py` 只负责“盯盘清单文案渲染”
- `action_memo.py` 只负责“操作备忘文案渲染”
- 两者共享同一个 holding snapshot DTO

### 5.3 `src/agents/overlay_report.py`

当前问题：

- 候选排序、中文解释、event context、LLM request export、报表落地都混在一个大文件里

目标拆法：

- `overlay_candidate_service.py`: 候选装配与打分
- `overlay_context_service.py`: 公告/新闻/研报上下文
- `overlay_writer.py`: packet/csv/md/jsonl 落盘
- `llm_gateway.py`: OpenAI 请求执行

这样未来无论是页面调用还是 CLI 调用，都能复用同一套应用服务。

## 6. 前端层怎么拆

### 6.1 页面拆分原则

每个 tab 对应一个 page module：

- `overview_page.py`
- `factors_page.py`
- `backtest_page.py`
- `candidates_page.py`
- `watchlist_page.py`
- `ai_page.py`
- `service_page.py`

每个 page 文件只做三件事：

1. 接收 service 输出的 view model
2. 绘制控件
3. 触发 action

不要在 page 里直接读 parquet / yaml / json。

### 6.2 组件拆分原则

以下内容适合抽成共享组件：

- 指标卡片组
- 排行榜表格
- 配置表单
- 状态提示条
- 日志展开框
- 持仓明细卡片

这样后面页面改版不会每个 tab 都重复维护。

### 6.3 ViewModel 原则

UI 层不要直接吃原始 DataFrame。

建议页面只依赖清晰的 view model，比如：

- `WatchlistSummaryVM`
- `WatchlistRowVM`
- `HoldingDetailVM`
- `OverlayCandidateVM`
- `DashboardStatusVM`

这会让页面更稳定，也更容易写测试。

## 7. 我推荐的重构顺序

### Phase 1：先把读取和装配抽出

目标：

- 不改页面功能
- 先把 `streamlit_app.py` 里的 `_load_*` 和 `_build_*` 下沉

优先动作：

1. 新建 `src/app/repositories/report_repository.py`
2. 新建 `src/app/repositories/config_repository.py`
3. 新建 `src/app/services/watchlist_service.py`
4. 新建 `src/app/services/dashboard_service.py`

完成标志：

- `streamlit_app.py` 不再直接读 parquet/yaml/json

### Phase 2：把 tab 拆成 page module

目标：

- 让 `streamlit_app.py` 只负责启动和装配

优先动作：

1. 把 7 个 tab 拆成 7 个 page 文件
2. 把共享表格和 metric 卡片拆到 `components`

完成标志：

- `streamlit_app.py` 控制在 300 行以内

### Phase 3：补 holding / overlay 的应用服务层

目标：

- 不再让 `action_memo.py` 依赖 `watch_plan.py` 私有函数

优先动作：

1. 提取 holding snapshot service
2. 提取 overlay candidate service
3. 提取 note rendering service

完成标志：

- `watch_plan.py` / `action_memo.py` 只负责文案和输出

### Phase 4：补测试护栏

目标：

- 给“应用层”补测试，而不是只测工具函数

优先动作：

1. `test_watchlist_service.py`
2. `test_dashboard_service.py`
3. `test_overlay_service.py`
4. `test_pipeline_runner.py`

完成标志：

- 页面核心装配逻辑不再只能靠手点验证

### Phase 5：再决定要不要拆真前后端

只有在下面场景出现时，才建议认真考虑 React/FastAPI：

- 需要多用户
- 需要权限管理
- 需要异步任务中心
- 需要更复杂的图表交互
- 需要盘中频繁刷新和任务调度

在那之前，先把分层单体做好，ROI 更高。

## 8. 我对当前项目的明确判断

### 该保留的

- `src/data / features / models / backtest` 这条研究管线
- 现有报表契约和文件产物
- Streamlit 作为短期交互壳

### 该尽快调整的

- `streamlit_app.py` 的总控脚本形态
- 持仓快照和 overlay 快照的共享逻辑
- 页面直接读文件的方式
- 以 DataFrame 字段名作为 UI 契约的方式

### 暂时不要做的

- 不要现在就大规模重命名整个 `src`
- 不要现在就直接切 React
- 不要一边重构一边继续把新功能堆进 `streamlit_app.py`

## 9. 我建议的第一批实际改造任务

如果要正式开工，我建议按这个顺序做：

1. 抽 `report_repository.py` 和 `config_repository.py`
2. 抽 `watchlist_service.py`
3. 把 `观察持仓` 页拆成独立 page
4. 提取 holding snapshot service
5. 给 `watchlist_service` 和 holding snapshot 补测试

这是我认为风险最低、收益最高的一批。

## 10. 本次研究的结论

一句话总结：

当前项目更像“研究管线 + 一个不断长大的脚本式 dashboard”，而不是“有应用层的产品化研究平台”。

所以最优解不是马上换前端，而是先把应用层补出来，让前端重新回到展示层的位置。
