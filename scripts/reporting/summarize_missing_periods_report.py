#!/usr/bin/env python3
"""complement_periods.txt を読み込んで欠損期間を自然言語でまとめるスクリプト。

欠損期間の統計情報や特徴を分析し、読みやすいレポートを生成します。
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

JST = timezone(timedelta(hours=9))
DEFAULT_PERIOD_FILE = Path(__file__).resolve().parents[2] / "periods" / "complement_periods.txt"


@dataclass
class MissingPeriod:
    """欠損期間を表すデータクラス"""

    start: datetime
    end: datetime

    def duration_minutes(self) -> int:
        """期間の長さ（分）を返す"""
        return int((self.end - self.start).total_seconds() / 60) + 10  # 終端スロット含む

    def duration_hours(self) -> float:
        """期間の長さ（時間）を返す"""
        return self.duration_minutes() / 60

    def is_single_slot(self) -> bool:
        """単一スロット（10分）かどうか"""
        return self.start == self.end

    def __str__(self) -> str:
        """人間が読みやすい形式で期間を表示"""
        start_str = self.start.strftime("%Y年%m月%d日 %H:%M")
        end_str = self.end.strftime("%Y年%m月%d日 %H:%M")

        if self.is_single_slot():
            return f"{start_str}（10分間）"
        else:
            duration = self.duration_minutes()
            if duration < 60:
                duration_str = f"{duration}分間"
            elif duration < 1440:  # 24時間未満
                hours = duration / 60
                duration_str = f"{hours:.1f}時間"
            else:
                days = duration / 1440
                duration_str = f"{days:.1f}日間"

            return f"{start_str} ～ {end_str}（{duration_str}）"


def parse_args() -> argparse.Namespace:
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(description="欠損期間を自然言語でまとめる")
    parser.add_argument(
        "--input",
        default=str(DEFAULT_PERIOD_FILE),
        help="入力ファイル（デフォルト: periods/complement_periods.txt）",
    )
    parser.add_argument(
        "--output",
        help="出力ファイル（指定しない場合は標準出力）",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="詳細な統計情報を表示",
    )
    return parser.parse_args()


def parse_timestamp(ts: str) -> datetime:
    """タイムスタンプをパース"""
    return datetime.strptime(ts, "%Y-%m-%d_%H-%M").replace(tzinfo=JST)


def load_periods(input_file: Path) -> List[MissingPeriod]:
    """欠損期間ファイルを読み込む

    2カラム形式（旧形式）と4カラム形式（新形式）の両方に対応:
    - 2カラム: 開始時刻,終了時刻
    - 4カラム: 開始時刻,終了時刻,開始ID,終了ID
    """
    periods = []

    with input_file.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split(",")

            # 2カラム形式または4カラム形式を受け入れる
            if len(parts) == 2:
                # 旧形式: 開始時刻,終了時刻
                start_ts, end_ts = parts[0], parts[1]
            elif len(parts) == 4:
                # 新形式: 開始時刻,終了時刻,開始ID,終了ID
                # 最初の2カラムだけを使用
                start_ts, end_ts = parts[0], parts[1]
            else:
                print(
                    f"警告: 無効な行をスキップ（2または4カラムを期待）: {line}",
                    file=sys.stderr
                )
                continue

            try:
                start = parse_timestamp(start_ts)
                end = parse_timestamp(end_ts)
                periods.append(MissingPeriod(start=start, end=end))
            except ValueError as e:
                print(
                    f"警告: タイムスタンプのパースに失敗: {line} (エラー: {e})",
                    file=sys.stderr
                )
                continue

    return periods


def analyze_periods(periods: List[MissingPeriod]) -> dict:
    """欠損期間を分析して統計情報を返す"""
    if not periods:
        return {}

    total_minutes = sum(p.duration_minutes() for p in periods)
    single_slots = [p for p in periods if p.is_single_slot()]
    multi_slots = [p for p in periods if not p.is_single_slot()]

    # 最長・最短期間
    longest = max(periods, key=lambda p: p.duration_minutes())
    shortest = min(periods, key=lambda p: p.duration_minutes())

    # 日付範囲
    earliest = min(p.start for p in periods)
    latest = max(p.end for p in periods)

    # 時間帯分析（時間別の欠損カウント）
    hourly_count = {}
    for p in periods:
        hour = p.start.hour
        hourly_count[hour] = hourly_count.get(hour, 0) + 1

    return {
        "total_periods": len(periods),
        "total_minutes": total_minutes,
        "total_hours": total_minutes / 60,
        "total_days": total_minutes / 1440,
        "single_slot_count": len(single_slots),
        "multi_slot_count": len(multi_slots),
        "longest_period": longest,
        "shortest_period": shortest,
        "earliest_date": earliest,
        "latest_date": latest,
        "hourly_count": hourly_count,
    }


def format_summary(periods: List[MissingPeriod], stats: dict, verbose: bool = False) -> str:
    """分析結果を自然言語でまとめる"""
    if not periods:
        return "欠損期間はありません。"

    lines = []
    lines.append("=" * 60)
    lines.append("データ欠損期間サマリー")
    lines.append("=" * 60)
    lines.append("")

    # 基本統計
    lines.append("【概要】")
    lines.append(f"・欠損期間の総数: {stats['total_periods']}期間")
    lines.append(f"・欠損時間の合計: {stats['total_hours']:.1f}時間（{stats['total_days']:.1f}日間）")
    lines.append(
        f"・対象期間: {stats['earliest_date'].strftime('%Y年%m月%d日')} ～ "
        f"{stats['latest_date'].strftime('%Y年%m月%d日')}"
    )
    lines.append("")

    # 期間の内訳
    lines.append("【期間の内訳】")
    lines.append(f"・単一スロット（10分）の欠損: {stats['single_slot_count']}期間")
    lines.append(f"・連続した欠損: {stats['multi_slot_count']}期間")
    lines.append("")

    # 最長・最短期間
    lines.append("【注目すべき期間】")
    lines.append(f"・最長の欠損: {stats['longest_period']}")
    if stats['shortest_period'] != stats['longest_period']:
        lines.append(f"・最短の欠損: {stats['shortest_period']}")
    lines.append("")

    # 詳細な期間リスト
    if verbose:
        lines.append("【すべての欠損期間】")
        for i, period in enumerate(periods, 1):
            lines.append(f"{i:3d}. {period}")
        lines.append("")

        # 時間帯分析
        if stats["hourly_count"]:
            lines.append("【時間帯別の欠損発生頻度】")
            sorted_hours = sorted(stats["hourly_count"].items())
            for hour, count in sorted_hours:
                lines.append(f"  {hour:02d}時台: {count}回")
            lines.append("")

    # まとめ
    lines.append("【まとめ】")

    if stats["total_days"] >= 1:
        lines.append(
            f"合計{stats['total_days']:.1f}日分のデータ欠損が確認されました。"
        )
    else:
        lines.append(
            f"合計{stats['total_hours']:.1f}時間分のデータ欠損が確認されました。"
        )

    if stats["multi_slot_count"] > 0:
        avg_duration = (
            sum(p.duration_minutes() for p in periods if not p.is_single_slot())
            / stats["multi_slot_count"]
        )
        lines.append(
            f"連続した欠損は平均{avg_duration:.0f}分間続いています。"
        )

    if stats["single_slot_count"] > stats["total_periods"] * 0.7:
        lines.append("大部分は単発の10分スロット欠損です。")
    elif stats["multi_slot_count"] > stats["total_periods"] * 0.7:
        lines.append("大部分は連続した長時間の欠損です。")

    lines.append("")
    lines.append("=" * 60)

    return "\n".join(lines)


def main() -> int:
    """メイン処理"""
    args = parse_args()
    input_file = Path(args.input)

    if not input_file.exists():
        print(f"エラー: ファイルが見つかりません: {input_file}", file=sys.stderr)
        return 1

    # 欠損期間を読み込み
    periods = load_periods(input_file)

    if not periods:
        print("欠損期間が見つかりませんでした。", file=sys.stderr)
        return 0

    # 分析
    stats = analyze_periods(periods)

    # サマリー生成
    summary = format_summary(periods, stats, verbose=args.verbose)

    # 出力
    if args.output:
        output_file = Path(args.output)
        output_file.write_text(summary, encoding="utf-8")
        print(f"サマリーを {output_file} に出力しました。")
    else:
        print(summary)

    return 0


if __name__ == "__main__":
    sys.exit(main())
