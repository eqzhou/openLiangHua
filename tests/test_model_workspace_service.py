from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.app.services.model_workspace_service import ModelWorkspace, build_model_panel


class ModelWorkspaceServiceTests(unittest.TestCase):
    def test_build_model_panel_uses_explicit_universe_when_loading_from_database(self) -> None:
        workspace = ModelWorkspace(
            root=Path("/Users/eqzhou/Public/openLiangHua"),
            data_source="tushare",
            experiment={"train_start": "2018-01-01", "test_end": "2026-04-01"},
            universe={
                "mode": "explicit",
                "symbols": ["000001.SZ", "000002.SZ"],
                "watch_symbols": ["000078.SZ"],
            },
        )
        fake_panel = pd.DataFrame([{"trade_date": "2026-04-01", "ts_code": "000001.SZ"}])

        with (
            patch("src.app.services.model_workspace_service._use_database_artifacts", return_value=True),
            patch("src.app.services.model_workspace_service._watchlist_symbols_for_model_universe", return_value=[]),
            patch("src.app.services.model_workspace_service.load_research_panel", return_value=fake_panel) as load_research_panel,
        ):
            panel = build_model_panel(workspace, date_from="2018-01-01", date_to="2026-04-01")

        self.assertEqual(len(panel), 1)
        self.assertEqual(
            load_research_panel.call_args.kwargs["symbols"],
            ["000001.SZ", "000002.SZ", "000078.SZ"],
        )
        self.assertEqual(load_research_panel.call_args.kwargs["date_from"], "2018-01-01")
        self.assertEqual(load_research_panel.call_args.kwargs["date_to"], "2026-04-01")

    def test_build_model_panel_uses_current_index_universe_when_loading_from_database(self) -> None:
        workspace = ModelWorkspace(
            root=Path("/Users/eqzhou/Public/openLiangHua"),
            data_source="tushare",
            experiment={},
            universe={
                "mode": "current_index",
                "index_code": "000905.SH",
                "watch_symbols": ["000078.SZ"],
            },
        )
        fake_panel = pd.DataFrame([{"trade_date": "2026-04-01", "ts_code": "000001.SZ"}])

        with (
            patch("src.app.services.model_workspace_service._use_database_artifacts", return_value=True),
            patch("src.app.services.model_workspace_service._watchlist_symbols_for_model_universe", return_value=[]),
            patch("src.app.services.model_workspace_service._current_index_symbols", return_value=("000001.SZ", "000002.SZ")),
            patch("src.app.services.model_workspace_service.load_research_panel", return_value=fake_panel) as load_research_panel,
        ):
            build_model_panel(workspace)

        self.assertEqual(
            load_research_panel.call_args.kwargs["symbols"],
            ["000001.SZ", "000002.SZ", "000078.SZ"],
        )

    def test_build_model_panel_does_not_default_to_bootstrap_user_watchlist(self) -> None:
        workspace = ModelWorkspace(
            root=Path("/Users/eqzhou/Public/openLiangHua"),
            data_source="tushare",
            experiment={},
            universe={
                "mode": "current_index",
                "index_code": "000905.SH",
                "watch_symbols": [],
            },
        )
        fake_panel = pd.DataFrame([{"trade_date": "2026-04-01", "ts_code": "000001.SZ"}])

        with (
            patch.dict("os.environ", {}, clear=True),
            patch("src.app.services.model_workspace_service._use_database_artifacts", return_value=True),
            patch("src.app.services.model_workspace_service.PostgresWatchlistStore", side_effect=AssertionError("should not read default user watchlist")),
            patch("src.app.services.model_workspace_service._current_index_symbols", return_value=("000001.SZ",)),
            patch("src.app.services.model_workspace_service.load_research_panel", return_value=fake_panel) as load_research_panel,
        ):
            build_model_panel(workspace)

        self.assertEqual(load_research_panel.call_args.kwargs["symbols"], ["000001.SZ"])

    def test_build_model_panel_prefers_database_watchlist_universe(self) -> None:
        workspace = ModelWorkspace(
            root=Path("/Users/eqzhou/Public/openLiangHua"),
            data_source="tushare",
            experiment={},
            universe={
                "mode": "current_index",
                "index_code": "000905.SH",
                "watch_symbols": ["000078.SZ"],
            },
        )
        fake_panel = pd.DataFrame([{"trade_date": "2026-04-01", "ts_code": "000078.SZ"}])

        with (
            patch("src.app.services.model_workspace_service._use_database_artifacts", return_value=True),
            patch(
                "src.app.services.model_workspace_service._watchlist_symbols_for_model_universe",
                return_value=["000078.SZ", "002008.SZ"],
            ) as load_watchlist_symbols,
            patch("src.app.services.model_workspace_service._current_index_symbols", side_effect=AssertionError("config universe should not be used")),
            patch("src.app.services.model_workspace_service.load_research_panel", return_value=fake_panel) as load_research_panel,
        ):
            build_model_panel(workspace, user_id="user-1")

        load_watchlist_symbols.assert_called_once_with("user-1")
        self.assertEqual(load_research_panel.call_args.kwargs["symbols"], ["000078.SZ", "002008.SZ"])

    def test_build_model_panel_rejects_empty_explicit_user_watchlist(self) -> None:
        workspace = ModelWorkspace(
            root=Path("/Users/eqzhou/Public/openLiangHua"),
            data_source="tushare",
            experiment={},
            universe={
                "mode": "current_index",
                "index_code": "000905.SH",
                "watch_symbols": ["000078.SZ"],
            },
        )

        with (
            patch("src.app.services.model_workspace_service._use_database_artifacts", return_value=True),
            patch("src.app.services.model_workspace_service._watchlist_symbols_for_model_universe", return_value=[]),
            patch("src.app.services.model_workspace_service._current_index_symbols", side_effect=AssertionError("must not fall back to config universe")),
            patch("src.app.services.model_workspace_service.load_research_panel", side_effect=AssertionError("must not load global panel")),
        ):
            with self.assertRaisesRegex(RuntimeError, "No watchlist_items symbols"):
                build_model_panel(workspace, user_id="user-empty")


if __name__ == "__main__":
    unittest.main()
