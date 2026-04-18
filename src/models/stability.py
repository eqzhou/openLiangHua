from __future__ import annotations

import json
from pathlib import Path

from src.app.repositories.report_repository import save_model_stability_report
from src.utils.data_source import source_prefixed_path


def build_stability_summary(metrics_by_split: dict[str, dict]) -> dict[str, object]:
    valid = metrics_by_split.get("valid", {}) or {}
    test = metrics_by_split.get("test", {}) or {}

    valid_return = float(valid.get("daily_portfolio_annualized_return", 0.0) or 0.0)
    test_return = float(test.get("daily_portfolio_annualized_return", 0.0) or 0.0)
    valid_drawdown = float(valid.get("daily_portfolio_max_drawdown", 0.0) or 0.0)
    test_drawdown = float(test.get("daily_portfolio_max_drawdown", 0.0) or 0.0)
    valid_turnover = float(valid.get("avg_turnover_ratio", 0.0) or 0.0)
    test_turnover = float(test.get("avg_turnover_ratio", 0.0) or 0.0)
    valid_rank_ic = float(valid.get("rank_ic_mean", 0.0) or 0.0)
    test_rank_ic = float(test.get("rank_ic_mean", 0.0) or 0.0)

    checks = {
        "valid_positive": valid_return > 0.0,
        "test_positive": test_return > 0.0,
        "valid_drawdown_ok": valid_drawdown > -0.20,
        "test_drawdown_ok": test_drawdown > -0.25,
        "valid_rank_ic_positive": valid_rank_ic > 0.0,
        "test_rank_ic_positive": test_rank_ic > 0.0,
        "turnover_ok": max(valid_turnover, test_turnover) <= 0.70,
        "return_gap_ok": abs(test_return - valid_return) <= 0.25,
    }
    passed = sum(bool(value) for value in checks.values())

    if passed >= 7:
        grade = "较稳"
        conclusion = "样本外稳定性较好，可以继续做更细的因子和事件增强。"
    elif passed >= 5:
        grade = "观察"
        conclusion = "结果有一定可用性，但还需要继续压回撤、控换手并做更细的样本外检验。"
    else:
        grade = "偏弱"
        conclusion = "当前样本外稳定性还不够，先不要把这套结果当成可交易策略。"

    return {
        "grade": grade,
        "conclusion": conclusion,
        "passed_checks": passed,
        "total_checks": len(checks),
        "checks": checks,
        "valid_return": valid_return,
        "test_return": test_return,
        "valid_drawdown": valid_drawdown,
        "test_drawdown": test_drawdown,
        "valid_turnover": valid_turnover,
        "test_turnover": test_turnover,
        "valid_rank_ic": valid_rank_ic,
        "test_rank_ic": test_rank_ic,
    }


def save_stability_summary(
    reports_dir: Path,
    model_name: str,
    data_source: str,
    metrics_by_split: dict[str, dict],
) -> dict[str, object]:
    summary = build_stability_summary(metrics_by_split)
    root = reports_dir.parent.parent if reports_dir.name == "weekly" and reports_dir.parent.name == "reports" else None
    if root is not None:
        save_model_stability_report(
            root,
            data_source=data_source,
            model_name=model_name,
            summary=summary,
        )
        return summary
    source_path = source_prefixed_path(reports_dir, f"{model_name}_stability.json", data_source)
    source_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (reports_dir / f"{model_name}_stability.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary
