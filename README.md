# stock-analyzer

A股股票分析 Claude Code Plugin，支持维护观察池和持仓池、抓取个股与概念板块数据、查询财报披露和减持公告风险，并输出结构化交易建议。

## 安装

```bash
npx skills add stock-analyzer
```

或手动安装：将本仓库克隆到 `~/.claude/plugins/stock-analyzer/`。

## 功能

- **股票池维护**：新增、移动、更新、删除观察池与持仓池条目，支持按名称反查代码、自动补全简称和概念板块
- **数据抓取**：获取个股行情快照、基本面、资金流、概念板块、板块龙头、风险事件
- **完整分析**：结合历史行情、实时行情、板块对标和风险提示，输出买入/观察/持有/减仓/卖出的建议
- **风险监控**：每日刷新财报披露、减持公告、负面新闻，支持 Windows 任务计划定时执行

## 使用方式

安装后，在 Claude Code 中直接对话即可触发 skill，例如：

- "帮我分析 002837"
- "把平安银行加到观察池"
- "分析我的持仓"
- "刷新今天的风险事件"

运行run_daily_refresh.bat手动刷新风险事件，运行register_daily_refresh_task.ps1添加到windows定时任务

## 依赖

- Python 3.x

## 插件结构

```
stock-analyzer/
  .claude-plugin/plugin.json
  skills/stock-analyzer/
    SKILL.md
    scripts/              # Python 脚本
    references/           # 评分规则、模板等参考文档
    agents/               # 子代理定义
    data/                 # 本地数据（观察池、持仓池、风险缓存）
  LICENSE
```

## 许可证

MIT