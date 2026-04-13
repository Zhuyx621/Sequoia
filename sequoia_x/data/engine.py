"""数据引擎模块：负责 SQLite 行情数据存储与 baostock 增量同步。"""

import atexit
import json
import math
import random
import sqlite3
import time
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd

from sequoia_x.core.config import Settings
from sequoia_x.core.logger import get_logger

logger = get_logger(__name__)


# 代码映射缓存：code -> 带交易所前缀代码（如 SH600519 / SZ000001 / BJ430047）
code_id_dict: dict[str, str] = {}
code_name_dict: dict[str, str] = {}
_MAPPING_CACHE_PATH = Path("data/code_mapping_cache.json")


def _to_prefixed_code(code: str) -> str:
    """将 6 位代码标准化为带交易所前缀的代码。"""
    if code.startswith("6"):
        return f"SH{code}"
    if code.startswith(("4", "8", "9")):
        return f"BJ{code}"
    return f"SZ{code}"


def _to_baostock_code(code: str) -> str:
    """将 6 位代码转换为 baostock 格式（如 sh.600519）。"""
    if code.startswith("6"):
        return f"sh.{code}"
    if code.startswith(("4", "8", "9")):
        return f"bj.{code}"
    return f"sz.{code}"


def _is_st_name(name: str) -> bool:
    """根据股票名称判断是否为 ST 股票。"""
    normalized = str(name).upper().replace(" ", "")
    return "ST" in normalized


def _parse_baostock_code(bs_code: str) -> tuple[str, str] | None:
    """解析 baostock code，返回 (6位代码, 前缀代码)。"""
    if "." not in bs_code:
        return None

    market, short = bs_code.lower().split(".", 1)
    if market not in {"sh", "sz", "bj"}:
        return None
    if len(short) != 6 or not short.isdigit():
        return None

    return short, f"{market.upper()}{short}"


def _read_mapping_cache(max_age_hours: int = 24) -> dict[str, str]:
    """读取本地映射缓存，若超时或损坏则返回空字典。"""
    try:
        if not _MAPPING_CACHE_PATH.exists():
            return {}

        payload = json.loads(_MAPPING_CACHE_PATH.read_text(encoding="utf-8"))
        ts = str(payload.get("updated_at", ""))
        mapping = payload.get("mapping", {})
        if not isinstance(mapping, dict) or not ts:
            return {}

        updated_at = datetime.fromisoformat(ts)
        age_hours = (datetime.now() - updated_at).total_seconds() / 3600
        if age_hours > max_age_hours:
            return {}

        return {str(k): str(v) for k, v in mapping.items()}
    except Exception as exc:
        logger.warning(f"读取代码映射缓存失败，将重建缓存：{exc}")
        return {}


def _read_name_cache(max_age_hours: int = 24) -> dict[str, str]:
    """读取股票名称缓存。"""
    try:
        if not _MAPPING_CACHE_PATH.exists():
            return {}

        payload = json.loads(_MAPPING_CACHE_PATH.read_text(encoding="utf-8"))
        ts = str(payload.get("updated_at", ""))
        names = payload.get("names", {})
        if not isinstance(names, dict) or not ts:
            return {}

        updated_at = datetime.fromisoformat(ts)
        age_hours = (datetime.now() - updated_at).total_seconds() / 3600
        if age_hours > max_age_hours:
            return {}

        return {str(k): str(v) for k, v in names.items()}
    except Exception as exc:
        logger.warning(f"读取股票名称缓存失败，将重建缓存：{exc}")
        return {}


def _write_mapping_cache(mapping: dict[str, str], names: dict[str, str] | None = None) -> None:
    """落盘代码映射缓存。"""
    try:
        _MAPPING_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "mapping": mapping,
        }
        if names is not None:
            payload["names"] = names
        _MAPPING_CACHE_PATH.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning(f"写入代码映射缓存失败：{exc}")


def _load_name_mapping_from_baostock() -> dict[str, str]:
    """仅加载 股票代码->股票名称 映射，用于补全名称缓存。"""
    try:
        import baostock as bs

        lg = bs.login()
        if str(lg.error_code) != "0":
            raise RuntimeError(f"baostock login failed: {lg.error_msg}")

        rs = bs.query_all_stock(day=datetime.now().strftime("%Y-%m-%d"))
        if str(rs.error_code) != "0":
            raise RuntimeError(f"baostock query_all_stock failed: {rs.error_msg}")

        rows: list[list[str]] = []
        while rs.next():
            rows.append(rs.get_row_data())

        df = pd.DataFrame(rows, columns=rs.fields)
        names: dict[str, str] = {}
        raw_codes = df.get("code", pd.Series(dtype="str")).astype(str).tolist()
        raw_names = df.get("code_name", pd.Series(dtype="str")).astype(str).tolist()
        for raw_code, raw_name in zip(raw_codes, raw_names):
            parsed = _parse_baostock_code(raw_code)
            if parsed is not None:
                short, _ = parsed
                names[short] = raw_name

        return names
    except Exception as exc:
        logger.warning(f"加载股票名称映射失败：{exc}")
        return {}
    finally:
        try:
            import baostock as bs

            bs.logout()
        except Exception:
            pass


def load_all_stock_mapping() -> dict[str, str]:
    """完整加载全市场股票代码映射，优先使用缓存，失败时回退网络。"""
    global code_name_dict
    cached = _read_mapping_cache(max_age_hours=24)
    cached_names = _read_name_cache(max_age_hours=24)
    if cached:
        if cached_names:
            code_name_dict = cached_names
        else:
            # 兼容旧缓存：若只有 code 映射没有 names，自动补全一次名称并回写缓存
            refreshed_names = _load_name_mapping_from_baostock()
            if refreshed_names:
                code_name_dict = refreshed_names
                _write_mapping_cache(cached, refreshed_names)
                logger.info(f"已补全股票名称缓存，共 {len(refreshed_names)} 条")
        return cached

    try:
        import baostock as bs
        from datetime import timedelta

        lg = bs.login()
        if str(lg.error_code) != "0":
            raise RuntimeError(f"baostock login failed: {lg.error_msg}")

        # 某些时段/节点 query_all_stock(day=today) 可能返回空，做多种回退尝试
        rows: list[list[str]] = []
        rs_fields: list[str] = []
        query_days = [
            datetime.now().strftime("%Y-%m-%d"),
            (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            None,
        ]
        for day in query_days:
            rs = bs.query_all_stock(day=day) if day else bs.query_all_stock()
            if str(rs.error_code) != "0":
                logger.warning(
                    f"query_all_stock(day={day}) 失败: {rs.error_msg}"
                )
                continue

            current_rows: list[list[str]] = []
            while rs.next():
                current_rows.append(rs.get_row_data())

            if current_rows:
                rows = current_rows
                rs_fields = rs.fields
                logger.info(
                    f"query_all_stock(day={day}) 成功，获取 {len(rows)} 条代码映射"
                )
                break

            logger.warning(f"query_all_stock(day={day}) 返回空结果，尝试下一种回退")

        if not rows:
            logger.warning("baostock 全市场映射为空，回退使用本地数据库 symbol 列表")
            fallback_symbols: list[str] = []
            try:
                with sqlite3.connect("data/sequoia_v2.db") as conn:
                    db_rows = conn.execute(
                        "SELECT DISTINCT symbol FROM stock_daily"
                    ).fetchall()
                fallback_symbols = [
                    str(row[0]) for row in db_rows
                    if row and len(str(row[0])) == 6 and str(row[0]).isdigit()
                ]
            except Exception as exc:
                logger.warning(f"读取本地 symbol 回退失败：{exc}")

            mapping = {symbol: _to_prefixed_code(symbol) for symbol in fallback_symbols}
            if mapping:
                _write_mapping_cache(mapping, {})
            return mapping

        df = pd.DataFrame(rows, columns=rs_fields)
        mapping: dict[str, str] = {}
        names: dict[str, str] = {}
        raw_codes = df.get("code", pd.Series(dtype="str")).astype(str).tolist()
        raw_names = df.get("code_name", pd.Series(dtype="str")).astype(str).tolist()
        for raw_code, raw_name in zip(raw_codes, raw_names):
            parsed = _parse_baostock_code(raw_code)
            if parsed is not None:
                short, prefixed = parsed
                mapping[short] = prefixed
                names[short] = raw_name

        if mapping:
            code_name_dict = names
            _write_mapping_cache(mapping, names)
        return mapping
    except Exception as exc:
        logger.warning(f"加载全市场代码映射失败：{exc}")
        return {}
    finally:
        try:
            import baostock as bs

            bs.logout()
        except Exception:
            pass


def init_code_mapping() -> None:
    """预加载完整股票代码映射表。"""
    global code_id_dict, code_name_dict
    loaded = load_all_stock_mapping()
    if loaded:
        code_id_dict = loaded
        code_name_dict = _read_name_cache(max_age_hours=24) or code_name_dict
        logger.info(f"代码映射表已预加载，共 {len(code_id_dict)} 条")
    else:
        logger.warning("代码映射表预加载失败，将在后续流程按需回退")


@dataclass
class SyncResult:
    """单个 symbol 同步结果。"""

    symbol: str
    status: Literal["success", "skip", "fail"]
    rows_added: int = 0


@dataclass
class SyncSummary:
    """全市场同步汇总统计。"""

    success: int = 0
    skipped: int = 0
    failed: int = 0


@dataclass
class PriceChangeStat:
    """推荐股票相对上一个交易日的涨跌统计。"""

    symbol: str
    name: str
    latest_date: str
    prev_date: str
    latest_close: float
    prev_close: float
    pct_change: float

    @property
    def direction(self) -> str:
        if self.pct_change > 0:
            return "上涨"
        if self.pct_change < 0:
            return "下跌"
        return "持平"


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol   TEXT    NOT NULL,
    date     TEXT    NOT NULL,
    open     REAL,
    high     REAL,
    low      REAL,
    close    REAL,
    volume   REAL,
    turnover REAL,
    UNIQUE (symbol, date)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
"""


class DataEngine:
    """行情数据引擎，负责 SQLite 存储和 baostock 增量同步。"""

    def __init__(self, settings: Settings) -> None:
        """
        初始化 DataEngine。

        Args:
            settings: 系统配置实例，提供 db_path 和 start_date。
        """
        self.db_path: str = settings.db_path
        self.start_date: str = settings.start_date
        # 请求稳态参数：降低接口被动断连概率
        self.max_symbol_retries: int = 3
        self.base_retry_sleep: float = 1.2
        self.request_interval_min: float = 0.22
        self.request_interval_max: float = 1
        self.cooldown_every: int = 120
        self.cooldown_min: float = 3.0
        self.cooldown_max: float = 8.0
        self._bs_logged_in: bool = False
        init_code_mapping()
        self._init_db()
        atexit.register(self._safe_logout_baostock)

    def _sleep_human_like(self, min_seconds: float, max_seconds: float) -> None:
        """随机暂停，模拟人工操作节奏。"""
        if max_seconds <= 0:
            return
        low = max(0.0, min_seconds)
        high = max(low, max_seconds)
        time.sleep(random.uniform(low, high))

    def _ensure_baostock_login(self) -> None:
        """确保 baostock 登录可用，避免每只股票重复登录。"""
        if self._bs_logged_in:
            return

        import baostock as bs

        lg = bs.login()
        if str(lg.error_code) != "0":
            raise RuntimeError(f"baostock login failed: {lg.error_msg}")
        self._bs_logged_in = True

    def _safe_logout_baostock(self) -> None:
        """安全退出 baostock 会话。"""
        if not self._bs_logged_in:
            return

        try:
            import baostock as bs

            bs.logout()
        except Exception:
            pass
        finally:
            self._bs_logged_in = False

    @staticmethod
    def _is_transient_network_error(exc: Exception) -> bool:
        """识别可通过重连恢复的网络类异常。"""
        text = str(exc)
        markers = [
            "WinError 10054",
            "远程主机强迫关闭了一个现有的连接",
            "网络接收错误",
            "接收数据异常",
            "Connection aborted",
            "RemoteDisconnected",
        ]
        return any(m in text for m in markers)

    def _fetch_ohlcv_from_baostock(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """从 baostock 拉取单只股票日线前复权数据。"""
        import baostock as bs

        self._ensure_baostock_login()
        rs = bs.query_history_k_data_plus(
            code=_to_baostock_code(symbol),
            fields="date,open,high,low,close,volume,amount",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",  # 前复权
        )
        if str(rs.error_code) != "0":
            raise RuntimeError(f"baostock query_history failed: {rs.error_msg}")

        rows: list[list[str]] = []
        while rs.next():
            rows.append(rs.get_row_data())

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=rs.fields)
        df = df.rename(columns={"amount": "turnover"})
        df["symbol"] = symbol
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["date"]).copy()
        keep_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "turnover"]
        return df[keep_cols]

    def get_symbol_name(self, symbol: str) -> str:
        """返回股票名称，缺失时回退为 未知。"""
        return code_name_dict.get(symbol, "未知")

    def is_st_stock(self, symbol: str) -> bool:
        """判断某股票是否为 ST。"""
        return _is_st_name(self.get_symbol_name(symbol))

    def _init_db(self) -> None:
        """
        初始化数据库：创建 data/ 目录、建表、建唯一索引。
        若表和索引已存在则跳过（幂等）。
        """
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(_CREATE_INDEX_SQL)
            conn.commit()
        logger.info(f"数据库初始化完成：{self.db_path}")

    def _get_last_date(self, symbol: str) -> str | None:
        """
        查询某 symbol 在本地数据库中的最新日期。

        Args:
            symbol: 股票代码，如 '000001'。

        Returns:
            最新日期字符串（格式 YYYY-MM-DD），无数据时返回 None。
        """
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM stock_daily WHERE symbol = ?",
                (symbol,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def get_ohlcv(self, symbol: str) -> pd.DataFrame:
        """
        读取某 symbol 的全量 OHLCV 数据，供策略层调用。

        Args:
            symbol: 股票代码。

        Returns:
            包含 date/open/high/low/close/volume/turnover 列的 DataFrame。
        """
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql(
                "SELECT * FROM stock_daily WHERE symbol = ? ORDER BY date",
                conn,
                params=(symbol,),
            )
        return df

    def sync_symbol(self, symbol: str) -> SyncResult:
        from datetime import date, timedelta

        last_date = self._get_last_date(symbol)
        today_date = date.today()
        today_str = today_date.strftime("%Y-%m-%d")

        if last_date is None:
            start = self.start_date
        else:
            last_date_obj = date.fromisoformat(last_date)
            # 👇 核心优化：如果本地数据已经是今天（或更晚），直接跳过，物理阻断网络请求！
            if last_date_obj >= today_date:
                return SyncResult(symbol=symbol, status="skip")

            start = (last_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")

        df = None
        last_exc: Exception | None = None
        for attempt in range(1, self.max_symbol_retries + 1):
            try:
                df = self._fetch_ohlcv_from_baostock(symbol, start_date=start, end_date=today_str)
                break
            except Exception as exc:
                last_exc = exc
                if self._is_transient_network_error(exc):
                    logger.warning(f"[{symbol}] 检测到网络断连，尝试重建 baostock 会话")
                    self._safe_logout_baostock()
                if attempt < self.max_symbol_retries:
                    backoff = self.base_retry_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.4)
                    logger.warning(
                        f"[{symbol}] baostock 拉取失败（第 {attempt}/{self.max_symbol_retries} 次）：{exc}；"
                        f"{backoff:.1f} 秒后重试"
                    )
                    time.sleep(backoff)

        if last_exc is not None and df is None:
            logger.warning(f"[{symbol}] baostock 拉取失败（重试后仍失败）：{last_exc}")
            return SyncResult(symbol=symbol, status="fail")

        if df is None or df.empty:
            return SyncResult(symbol=symbol, status="skip")

        rows = len(df)
        try:
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(
                    "stock_daily",
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
        except sqlite3.IntegrityError as exc:
            logger.warning(f"[{symbol}] 写入时遇到重复数据，已跳过：{exc}")

        return SyncResult(symbol=symbol, status="success", rows_added=rows)

    def get_all_symbols(self) -> list[str]:
        """
        从 baostock 获取全市场 A 股 symbol 列表（轻量接口）。
        包含网络重试机制，防止服务器掐断连接。

        Returns:
            股票代码字符串列表，如 ['000001', '000002', ...]。
        """
        global code_id_dict
        import time

        # 优先使用预加载映射，减少重复网络请求
        if code_id_dict:
            symbols = sorted(code_id_dict.keys())
            logger.info(f"从代码映射缓存加载股票列表，共 {len(symbols)} 只")
            return symbols

        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"正在获取全市场股票列表 (第 {attempt + 1}/{max_retries} 次尝试)...")
                mapping = load_all_stock_mapping()
                if not mapping:
                    raise RuntimeError("股票代码映射为空")

                symbols = sorted(mapping.keys())

                # 回填全局映射与本地缓存，供后续流程复用
                code_id_dict = mapping
                _write_mapping_cache(code_id_dict)

                logger.info(f"成功获取股票列表，共 {len(symbols)} 只股票。")
                return symbols
            except Exception as e:
                logger.warning(f"获取全市场列表失败: {e}。3秒后重试...")
                time.sleep(3)

        logger.error("获取全市场列表最终失败！请检查网络连接。")
        # 兜底：即使远端失败，也尽量使用本地库继续跑策略
        local_symbols = self.get_local_symbols(exclude_st=False)
        if local_symbols:
            logger.warning(f"回退使用本地股票池，共 {len(local_symbols)} 只")
            return local_symbols
        return []

    def get_local_symbols(self, exclude_st: bool = True) -> list[str]:
        """
        从本地 SQLite 数据库获取已有数据的股票代码列表，无需网络请求。

        Returns:
            本地已存在数据的股票代码列表。
        """
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM stock_daily"
            ).fetchall()
        symbols = [row[0] for row in rows]
        if not exclude_st:
            return symbols

        filtered = [symbol for symbol in symbols if not self.is_st_stock(symbol)]
        if len(filtered) != len(symbols):
            logger.info(f"已过滤 ST 股票 {len(symbols) - len(filtered)} 只")
        return filtered

    def get_price_change_stats(self, symbols: list[str]) -> list[PriceChangeStat]:
        """统计推荐股票相对上一个交易日的涨跌幅。"""
        stats: list[PriceChangeStat] = []
        for symbol in symbols:
            df = self.get_ohlcv(symbol)
            if df.empty:
                continue

            # 清洗脏数据，避免 close/date 异常导致 float 转换失败。
            work = df[["date", "close"]].copy()
            work["date"] = pd.to_datetime(work["date"], errors="coerce")
            work["close"] = pd.to_numeric(work["close"], errors="coerce")
            work = work.dropna(subset=["date", "close"]).sort_values("date")

            if len(work) < 2:
                continue

            prev = work.iloc[-2]
            latest = work.iloc[-1]
            prev_close = float(prev["close"])
            latest_close = float(latest["close"])
            if prev_close <= 0:
                continue
            if not math.isfinite(prev_close) or not math.isfinite(latest_close):
                continue

            pct_change = (latest_close - prev_close) / prev_close * 100
            stats.append(
                PriceChangeStat(
                    symbol=symbol,
                    name=self.get_symbol_name(symbol),
                    latest_date=latest["date"].strftime("%Y-%m-%d"),
                    prev_date=prev["date"].strftime("%Y-%m-%d"),
                    latest_close=latest_close,
                    prev_close=prev_close,
                    pct_change=pct_change,
                )
            )

        return stats

    def sync_all(self, symbols: list[str]) -> SyncSummary:
        """
        批量增量同步全市场，展示 rich 进度条。

        Args:
            symbols: 股票代码列表，通常由 get_all_symbols() 提供。

        Returns:
            SyncSummary，包含 success / skipped / failed 计数。
        """
        from rich.progress import (
            BarColumn,
            MofNCompleteColumn,
            Progress,
            SpinnerColumn,
            TextColumn,
            TimeElapsedColumn,
        )

        summary = SyncSummary()
        first_round_failed: list[str] = []
        consecutive_failures = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold cyan]同步中[/bold cyan]"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            TextColumn("[yellow]{task.fields[symbol]}[/yellow]"),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task("sync", total=len(symbols), symbol="")

            for symbol in symbols:
                f"正在同步: {symbol} "
                progress.update(task, symbol=symbol)
                result = self.sync_symbol(symbol)

                if result.status == "success":
                    summary.success += 1
                    consecutive_failures = 0
                elif result.status == "skip":
                    summary.skipped += 1
                    consecutive_failures = 0
                else:
                    summary.failed += 1
                    first_round_failed.append(symbol)
                    consecutive_failures += 1

                progress.advance(task)
                # 轻微节流，降低被上游接口中断概率
                self._sleep_human_like(self.request_interval_min, self.request_interval_max)

                # 周期性长休眠，避免持续高频请求触发风控
                if summary.success + summary.skipped + summary.failed:
                    processed = summary.success + summary.skipped + summary.failed
                    if processed % self.cooldown_every == 0:
                        cooldown = random.uniform(self.cooldown_min, self.cooldown_max)
                        logger.info(
                            f"已处理 {processed} 只股票，进入冷却 {cooldown:.1f} 秒"
                        )
                        time.sleep(cooldown)

                # 连续失败熔断：防止会话异常或上游风控时持续高频撞库
                if consecutive_failures >= 20:
                    cooldown = random.uniform(12.0, 24.0)
                    logger.warning(
                        f"连续失败 {consecutive_failures} 只股票，触发熔断冷却 {cooldown:.1f} 秒并重连会话"
                    )
                    self._safe_logout_baostock()
                    time.sleep(cooldown)
                    consecutive_failures = 0

        # 第一轮失败的股票自动二轮补拉
        rescued_success = 0
        rescued_skip = 0
        remaining_failed: list[str] = []
        if first_round_failed:
            logger.info(f"第一轮失败 {len(first_round_failed)} 只，开始自动二轮补拉...")
            for symbol in first_round_failed:
                result = self.sync_symbol(symbol)
                if result.status == "success":
                    summary.success += 1
                    summary.failed -= 1
                    rescued_success += 1
                elif result.status == "skip":
                    summary.skipped += 1
                    summary.failed -= 1
                    rescued_skip += 1
                else:
                    remaining_failed.append(symbol)

                self._sleep_human_like(self.request_interval_min, self.request_interval_max)

            logger.info(
                f"二轮补拉完成 — 转成功: {rescued_success} | "
                f"转跳过: {rescued_skip} | "
                f"仍失败: {len(remaining_failed)}"
            )

        # 自动落盘仍失败代码，便于后续断点补拉
        failed_dump_path = Path(self.db_path).parent / (
            f"failed_symbols_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with failed_dump_path.open("w", encoding="utf-8") as f:
            json.dump(remaining_failed, f, ensure_ascii=False, indent=2)
        logger.info(
            f"失败代码已落盘：{failed_dump_path.as_posix()} "
            f"(共 {len(remaining_failed)} 只)"
        )

        logger.info(
            f"同步完成 — 成功: {summary.success} | "
            f"跳过: {summary.skipped} | "
            f"失败: {summary.failed}"
        )
        return summary
