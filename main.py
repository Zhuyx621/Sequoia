"""Sequoia-X-X V2 主程序入口。

调度顺序：初始化配置 → 初始化日志 → 数据同步 → 策略执行 → 结果推送。
"""

import sys
import json
from dotenv import load_dotenv
load_dotenv()

from datetime import date
from pathlib import Path

import socket
socket.setdefaulttimeout(10.0)

from sequoia_x.core.config import get_settings
from sequoia_x.core.logger import get_logger
from sequoia_x.data.engine import DataEngine
from sequoia_x.notify.feishu import FeishuNotifier
from sequoia_x.strategy.base import BaseStrategy
from sequoia_x.strategy.high_tight_flag import HighTightFlagStrategy
from sequoia_x.strategy.limit_up_shakeout import LimitUpShakeoutStrategy
from sequoia_x.strategy.ma_volume import MaVolumeStrategy
from sequoia_x.strategy.turtle_trade import TurtleTradeStrategy
from sequoia_x.strategy.uptrend_limit_down import UptrendLimitDownStrategy
from sequoia_x.strategy.rps_breakout import RpsBreakoutStrategy


_PREDICTION_HISTORY_PATH = Path("data/prediction_history.json")


def _load_prediction_history() -> dict[str, dict[str, list[str]]]:
    """读取历史预测记录。"""
    try:
        if not _PREDICTION_HISTORY_PATH.exists():
            return {}
        payload = json.loads(_PREDICTION_HISTORY_PATH.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {}
        history: dict[str, dict[str, list[str]]] = {}
        for day, per_strategy in payload.items():
            if not isinstance(per_strategy, dict):
                continue
            normalized: dict[str, list[str]] = {}
            for strategy_name, symbols in per_strategy.items():
                if isinstance(symbols, list):
                    normalized[str(strategy_name)] = [str(s) for s in symbols]
            history[str(day)] = normalized
        return history
    except Exception:
        return {}


def _save_prediction_history(history: dict[str, dict[str, list[str]]]) -> None:
    """保存历史预测记录。"""
    _PREDICTION_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    _PREDICTION_HISTORY_PATH.write_text(
        json.dumps(history, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _get_previous_prediction_day(
    history: dict[str, dict[str, list[str]]],
    today: str,
) -> str | None:
    """返回今天之前最近一次有记录的日期。"""
    previous_days = [day for day in history.keys() if day < today]
    if not previous_days:
        return None
    return sorted(previous_days)[-1]


def _format_stat_items(items: list[str], max_items: int = 12) -> str:
    """格式化输出列表，避免单条日志过长。"""
    if not items:
        return "无"
    if len(items) <= max_items:
        return "，".join(items)
    return "，".join(items[:max_items]) + f"，... 共 {len(items)} 只"


def main() -> None:
    """
    主调度函数，按顺序执行完整的数据同步和选股流程。

    流程：
    1. 加载并校验配置（ValidationError 时终止）
    2. 初始化日志
    3. 初始化数据引擎并执行全市场增量同步
    4. 遍历所有策略依次执行选股
    5. 有选股结果时推送至对应飞书机器人

    Raises:
        SystemExit: 任意阶段发生未捕获异常时，以退出码 1 终止进程。
    """
    try:
        # 1. 初始化配置
        settings = get_settings()

        # 2. 初始化日志
        logger = get_logger(__name__)
        logger.info("Sequoia-X-X V2 启动")
        today_str = date.today().strftime("%Y-%m-%d")

        prediction_history = _load_prediction_history()
        previous_day = _get_previous_prediction_day(prediction_history, today_str)
        if previous_day:
            logger.info(f"检测到历史预测，最近一次为 {previous_day}")
        else:
            logger.info("未检测到历史预测，将从今日开始建立准确率统计")

        # 3. 数据同步
        engine = DataEngine(settings)
        if date.today().weekday() < 5:  # 周一到周五：0, 1, 2, 3, 4
            logger.info("工作日，开始增量同步最新数据...")
            all_symbols = engine.get_all_symbols()
            summary = engine.sync_all(all_symbols)
            logger.info(
                f"数据同步完成 — 成功: {summary.success} | "
                f"跳过: {summary.skipped} | 失败: {summary.failed}"
            )
        else:
            logger.info("🌟 今天是周末，A股休市！直接跳过网络拉取，使用本地最新数据极速调试策略！")

        # 4. 策略列表（新增策略在此追加即可）
        strategies: list[BaseStrategy] = [
            MaVolumeStrategy(engine=engine, settings=settings),
            TurtleTradeStrategy(engine=engine, settings=settings),
            HighTightFlagStrategy(engine=engine, settings=settings),
            LimitUpShakeoutStrategy(engine=engine, settings=settings),
            UptrendLimitDownStrategy(engine=engine, settings=settings),
            RpsBreakoutStrategy(engine=engine, settings=settings),
        ]

        # 本轮策略的实际筛选范围（来自本地数据库已落库股票）
        screened_total = len(engine.get_local_symbols())
        logger.info(f"本轮可筛选股票池：{screened_total} 只")

        notifier = FeishuNotifier(settings)
        strategies_with_result = 0
        today_predictions: dict[str, list[str]] = {}

        # 5. 遍历策略，有结果则推送至对应机器人
        for strategy in strategies:
            strategy_name = type(strategy).__name__
            logger.info(f"执行策略：{strategy_name}")

            selected: list[str] = strategy.run()
            selected = sorted(set(selected))
            today_predictions[strategy_name] = selected
            logger.info(f"{strategy_name} 选出 {len(selected)} 只股票")

            if previous_day:
                previous_selected = prediction_history.get(previous_day, {}).get(strategy_name, [])
                if previous_selected:
                    prev_stats = engine.get_price_change_stats(previous_selected)
                    if prev_stats:
                        up_items = [
                            f"{item.symbol}{item.name}({item.pct_change:+.2f}%)"
                            for item in prev_stats if item.pct_change > 0
                        ]
                        down_items = [
                            f"{item.symbol}{item.name}({item.pct_change:+.2f}%)"
                            for item in prev_stats if item.pct_change < 0
                        ]
                        flat_items = [
                            f"{item.symbol}{item.name}(0.00%)"
                            for item in prev_stats if item.pct_change == 0
                        ]
                        win_rate = len(up_items) / len(prev_stats) * 100

                        logger.info(
                            f"{strategy_name} 昨日预测准确率({previous_day}->{today_str}) — "
                            f"可评估: {len(prev_stats)} | 上涨: {len(up_items)} | "
                            f"下跌: {len(down_items)} | 持平: {len(flat_items)} | "
                            f"严格准确率(仅上涨): {win_rate:.2f}%"
                        )
                        logger.info(f"{strategy_name} 昨日上涨明细：{_format_stat_items(up_items)}")
                        logger.info(f"{strategy_name} 昨日下跌明细：{_format_stat_items(down_items)}")

                    overlap = sorted(set(previous_selected) & set(selected))
                    dropped = sorted(set(previous_selected) - set(selected))
                    added = sorted(set(selected) - set(previous_selected))
                    logger.info(
                        f"{strategy_name} 与今日预测对比 — 重合: {len(overlap)} | "
                        f"昨日命中但今日未入选: {len(dropped)} | 今日新增: {len(added)}"
                    )
                    logger.info(f"{strategy_name} 重合明细：{_format_stat_items(overlap)}")
                else:
                    logger.info(
                        f"{strategy_name} 无昨日预测记录（{previous_day}），"
                        "跳过昨日准确率与重合度统计"
                    )
            else:
                logger.info(
                    f"{strategy_name} 未找到历史基准日，"
                    "跳过昨日准确率与重合度统计"
                )

            if selected:
                strategies_with_result += 1
                # 对入选股票做一次小批量增量同步，降低个别票数据滞后一天的概率。
                selected_sync = engine.sync_all(selected)
                logger.info(
                    f"{strategy_name} 入选股票补同步完成 — "
                    f"成功: {selected_sync.success} | 跳过: {selected_sync.skipped} | 失败: {selected_sync.failed}"
                )

                stats = engine.get_price_change_stats(selected)
                if stats:
                    up_count = sum(1 for item in stats if item.pct_change > 0)
                    down_count = sum(1 for item in stats if item.pct_change < 0)
                    flat_count = len(stats) - up_count - down_count
                    logger.info(
                        f"{strategy_name} 相对上个交易日统计 — "
                        f"上涨: {up_count} | 下跌: {down_count} | 持平: {flat_count}"
                    )
                    for item in stats:
                        logger.info(
                            f"{strategy_name} | {item.symbol} {item.name} | "
                            f"{item.prev_date}->{item.latest_date} | "
                            f"{item.direction} {item.pct_change:+.2f}% | "
                            f"{item.prev_close:.2f}->{item.latest_close:.2f}"
                        )

                notifier.send(
                    symbols=selected,
                    strategy_name=strategy_name,
                    webhook_key=strategy.webhook_key,
                    price_stats=stats,
                )
            else:
                logger.info(f"{strategy_name} 无选股结果，跳过推送")

        logger.info(
            f"本轮共执行 {len(strategies)} 个策略；"
            f"有结果策略 {strategies_with_result} 个；"
            f"总共筛选股票 {screened_total} 只"
        )

        prediction_history[today_str] = today_predictions
        _save_prediction_history(prediction_history)
        logger.info(f"今日预测已写入 {_PREDICTION_HISTORY_PATH.as_posix()}")

    except Exception:
        try:
            _logger = get_logger(__name__)
            _logger.exception("主流程发生未捕获异常，程序终止")
        except Exception:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    logger.info("Sequoia-X-X V2 运行完成")


if __name__ == "__main__":
    main()
