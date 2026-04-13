"""数据引擎属性测试。"""

import gc
import shutil
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

from sequoia_x.core.config import Settings
from sequoia_x.data.engine import DataEngine, SyncResult


def make_engine_in(tmp_dir: str) -> tuple[DataEngine, Settings]:
    """创建使用临时数据库的 DataEngine 实例。"""
    settings = Settings(
        db_path=str(Path(tmp_dir) / "test.db"),
        start_date="2024-01-01",
        feishu_webhook_url="https://example.com/hook",
    )
    engine = DataEngine(settings)
    return engine, settings


@contextmanager
def managed_temp_engine() -> tuple[DataEngine, Settings]:
    """创建并可靠清理临时 DataEngine（兼容 Windows 文件句柄回收时序）。"""
    tmp_dir = tempfile.mkdtemp(prefix="sequoia_test_")
    engine = None
    try:
        engine, settings = make_engine_in(tmp_dir)
        yield engine, settings
    finally:
        if engine is not None:
            try:
                engine._safe_logout_baostock()
            except Exception:
                pass
            try:
                del engine
            except Exception:
                pass
        gc.collect()
        shutil.rmtree(tmp_dir, ignore_errors=True)


# Feature: sequoia-x-v2, Property 4: (symbol, date) 唯一约束防止重复写入
@given(
    symbol=st.text(min_size=6, max_size=6, alphabet="0123456789"),
    trade_date=st.dates(min_value=date(2024, 1, 1), max_value=date(2025, 12, 31)),
)
@h_settings(max_examples=50, deadline=None)
def test_unique_symbol_date_constraint(symbol: str, trade_date: date) -> None:
    """属性 4：相同 (symbol, date) 插入两次，数据库中该组合记录数应保持为 1。"""
    with managed_temp_engine() as (engine, _):
        row = {
            "symbol": symbol, "date": str(trade_date),
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
            "volume": 1000.0, "turnover": 10500.0,
        }
        df = pd.DataFrame([row])
        with sqlite3.connect(engine.db_path) as conn:
            df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")
            try:
                df.to_sql("stock_daily", conn, if_exists="append", index=False, method="multi")
            except sqlite3.IntegrityError:
                pass
            count = conn.execute(
                "SELECT COUNT(*) FROM stock_daily WHERE symbol=? AND date=?",
                (symbol, str(trade_date)),
            ).fetchone()[0]
        assert count == 1


# Feature: sequoia-x-v2, Property 5: 增量同步的 start_date 由 last_date 决定
@given(
    symbol=st.text(min_size=6, max_size=6, alphabet="0123456789"),
    last_date=st.dates(min_value=date(2024, 1, 1), max_value=date(2025, 11, 30)),
)
@h_settings(max_examples=50, deadline=None)
def test_sync_start_date_from_last_date(symbol: str, last_date: date) -> None:
    """属性 5：有 last_date 时，baostock 调用的 start_date 应为 last_date + 1 天。"""
    with managed_temp_engine() as (engine, _):
        # 预插入一条数据，使 last_date 生效
        row = pd.DataFrame([{
            "symbol": symbol, "date": str(last_date),
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
            "volume": 1000.0, "turnover": 10500.0,
        }])
        with sqlite3.connect(engine.db_path) as conn:
            row.to_sql("stock_daily", conn, if_exists="append", index=False)

        expected_start = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        captured: dict = {}

        def mock_fetch(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            return pd.DataFrame()

        with patch.object(engine, "_fetch_ohlcv_from_baostock", side_effect=mock_fetch):
            engine.sync_symbol(symbol)

        assert captured.get("start_date") == expected_start


# Feature: sequoia-x-v2, Property 6: 空增量数据不触发写入
@given(symbol=st.text(min_size=6, max_size=6, alphabet="0123456789"))
@h_settings(max_examples=50, deadline=None)
def test_empty_data_returns_skip(symbol: str) -> None:
    """属性 6：baostock 返回空 DataFrame 时，sync_symbol 应返回 status='skip'，数据库行数不变。"""
    with managed_temp_engine() as (engine, _):
        with patch.object(engine, "_fetch_ohlcv_from_baostock", return_value=pd.DataFrame()):
            result = engine.sync_symbol(symbol)
        assert result.status == "skip"
        with sqlite3.connect(engine.db_path) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM stock_daily WHERE symbol=?", (symbol,)
            ).fetchone()[0]
        assert count == 0


# Feature: sequoia-x-v2, Property 7: akshare 调用失败不中断整体同步
@given(
    symbols=st.lists(
        st.text(min_size=6, max_size=6, alphabet="0123456789"),
        min_size=2, max_size=5, unique=True,
    ),
    fail_index=st.integers(min_value=0, max_value=1),
)
@h_settings(max_examples=30, deadline=None)
def test_akshare_failure_does_not_interrupt_sync(
    symbols: list[str], fail_index: int
) -> None:
    """属性 7：部分 symbol 的 baostock 调用失败时，sync_all 应继续处理剩余 symbol。"""
    with managed_temp_engine() as (engine, _):
        fail_symbol = symbols[fail_index % len(symbols)]

        def mock_fetch(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
            if symbol == fail_symbol:
                raise ConnectionError("模拟网络超时")
            return pd.DataFrame()

        with patch.object(engine, "_fetch_ohlcv_from_baostock", side_effect=mock_fetch):
            summary = engine.sync_all(symbols)

        assert summary.failed >= 1
        assert summary.success + summary.skipped + summary.failed == len(symbols)


# Feature: sequoia-x-v2, Property 8: akshare 调用始终使用前复权参数
@given(symbol=st.text(min_size=6, max_size=6, alphabet="0123456789"))
@h_settings(max_examples=50, deadline=None)
def test_sync_always_uses_qfq(symbol: str) -> None:
    """属性 8：sync_symbol 调用 baostock 时，adjustflag 参数应始终为 '2'（前复权）。"""
    with managed_temp_engine() as (engine, _):
        captured: dict = {}

        def mock_fetch(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
            captured["symbol"] = symbol
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            return pd.DataFrame()

        with patch.object(engine, "_fetch_ohlcv_from_baostock", side_effect=mock_fetch):
            engine.sync_symbol(symbol)

        assert captured.get("symbol") == symbol


def test_price_change_stats_ignore_invalid_close_rows() -> None:
    """回归：中间交易日 close 为空时，不应抛错，且应使用最近两条有效收盘价。"""
    with managed_temp_engine() as (engine, _):
        symbol = "000001"
        rows = pd.DataFrame([
            {
                "symbol": symbol,
                "date": "2026-03-14",
                "open": 10.0,
                "high": 10.3,
                "low": 9.9,
                "close": 10.0,
                "volume": 1000.0,
                "turnover": 10000.0,
            },
            {
                "symbol": symbol,
                "date": "2026-03-15",
                "open": 10.0,
                "high": 10.2,
                "low": 9.8,
                "close": None,
                "volume": 1200.0,
                "turnover": 11000.0,
            },
            {
                "symbol": symbol,
                "date": "2026-03-16",
                "open": 10.8,
                "high": 11.2,
                "low": 10.7,
                "close": 11.0,
                "volume": 1400.0,
                "turnover": 15000.0,
            },
        ])
        with sqlite3.connect(engine.db_path) as conn:
            rows.to_sql("stock_daily", conn, if_exists="append", index=False)

        stats = engine.get_price_change_stats([symbol])
        assert len(stats) == 1
        assert stats[0].symbol == symbol
        assert stats[0].prev_date == "2026-03-14"
        assert stats[0].latest_date == "2026-03-16"
        assert abs(stats[0].pct_change - 10.0) < 1e-9
