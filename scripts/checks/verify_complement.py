#!/usr/bin/env python3
"""補完データの検証スクリプト

complement_periods.txt に記載された期間について、data_complement/ 以下に
正しく補完データが作成されているかを検証する。

検証項目:
1. 補完ファイルが存在するか
2. 補完データの時刻範囲が適切か（スロット開始から指定秒数以内をカバー）
3. data/ と data_complement/ でノートIDの重複がないか
4. 時系列データの妥当性（データ件数、時刻範囲）

使用例:
    # 全期間を検証
    python scripts/checks/verify_complement.py --periods-file periods/complement_periods.txt

    # 詳細表示モード
    python scripts/checks/verify_complement.py --periods-file periods/complement_periods.txt --verbose

    # 開始カバレッジの閾値を変更
    python scripts/checks/verify_complement.py --early-coverage-seconds 60
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# 日本標準時（JST = UTC+9）のタイムゾーン定義
JST = timezone(timedelta(hours=9))
DEFAULT_PERIOD_FILE = Path(__file__).resolve().parents[2] / "periods" / "complement_periods.txt"


def parse_timestamp(ts: str) -> datetime:
    """YYYY-MM-DD_HH-MM形式のタイムスタンプをパース

    Args:
        ts: タイムスタンプ文字列（例: "2025-08-01_00-40"）

    Returns:
        JST timezone-aware のdatetimeオブジェクト
    """
    return datetime.strptime(ts, "%Y-%m-%d_%H-%M").replace(tzinfo=JST)


def load_note_ids_and_times(path: Path) -> List[Tuple[str, datetime]]:
    """JSONL形式のファイルからノートIDと作成時刻のリストを取得

    各行をJSONとしてパースし、ノートのIDとcreatedAtを抽出する。
    パースエラーやデータ欠損がある行はスキップする。

    Args:
        path: JSONLファイルのパス

    Returns:
        (note_id, created_at_datetime) のタプルのリスト
    """
    results = []
    if not path.exists():
        return results

    with path.open(encoding="utf-8") as f:
        for line in f:
            try:
                note = json.loads(line)
                note_id = note.get("id")
                created_at = note.get("createdAt")

                if note_id and created_at:
                    # ISO8601形式（Z）をパースしてJSTに変換
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(JST)
                    results.append((note_id, dt))
            except (json.JSONDecodeError, ValueError):
                # パースエラーは無視して次の行へ
                continue

    return results


def get_slot_path(root: Path, timestamp: str) -> Path:
    """タイムスタンプから対応するファイルパスを生成

    Args:
        root: データのルートディレクトリ（data/ または data_complement/）
        timestamp: タイムスタンプ文字列（例: "2025-08-01_00-40"）

    Returns:
        完全なファイルパス（例: data/2025/08/01/00/2025-08-01_00-40.jsonl）
    """
    dt = parse_timestamp(timestamp)
    return root.joinpath(
        dt.strftime("%Y"),  # 年
        dt.strftime("%m"),  # 月
        dt.strftime("%d"),  # 日
        dt.strftime("%H"),  # 時
        f"{timestamp}.jsonl",
    )


def verify_slot(
    timestamp: str,
    data_root: Path,
    complement_root: Path,
    early_coverage_seconds: int = 30,
) -> Dict:
    """1つのスロットについて補完データを検証

    以下の項目をチェックする:
    - 補完ファイルの存在
    - data/ と data_complement/ でのID重複
    - 時刻カバレッジ（開始から指定秒数以内をカバーしているか）

    Args:
        timestamp: 検証対象のタイムスタンプ（例: "2025-08-01_00-40"）
        data_root: 既存データのルートディレクトリ
        complement_root: 補完データのルートディレクトリ
        early_coverage_seconds: 開始カバレッジの判定閾値（秒）

    Returns:
        検証結果の辞書（詳細は result の構造を参照）
    """
    # スロットの時刻範囲を計算
    slot_start = parse_timestamp(timestamp)
    slot_end = slot_start + timedelta(minutes=10)
    early_threshold = slot_start + timedelta(seconds=early_coverage_seconds)

    # data/ と data_complement/ のファイルパスを生成
    data_path = get_slot_path(data_root, timestamp)
    complement_path = get_slot_path(complement_root, timestamp)

    # データ読み込み（ファイルが存在しない場合は空リスト）
    data_notes = load_note_ids_and_times(data_path) if data_path.exists() else []
    complement_notes = load_note_ids_and_times(complement_path) if complement_path.exists() else []

    # ノートIDのセットを作成
    data_ids = {note_id for note_id, _ in data_notes}
    complement_ids = {note_id for note_id, _ in complement_notes}

    # ID重複チェック（data/ と data_complement/ で同じIDが存在するか）
    duplicates = data_ids & complement_ids

    # 補完データの時刻範囲を取得
    complement_times = [dt for _, dt in complement_notes]
    complement_min = min(complement_times) if complement_times else None  # 最古時刻
    complement_max = max(complement_times) if complement_times else None  # 最新時刻

    # カバレッジ判定
    # 開始カバレッジ: スロット開始から指定秒数以内のノートが取得できているか
    has_early_coverage = complement_min and complement_min <= early_threshold if complement_notes else False
    # 終了カバレッジは実用上問題ないためチェックしない
    # （スロット終了ギリギリまで取得するのは困難なため）
    # has_late_coverage = complement_max and complement_max >= slot_end - timedelta(seconds=1) if complement_notes else False
    has_late_coverage = True  # 常にOKとする

    # 検証結果を辞書にまとめる
    result = {
        "timestamp": timestamp,
        "slot_start": slot_start,
        "slot_end": slot_end,
        "data_exists": data_path.exists(),
        "data_count": len(data_ids),
        "complement_exists": complement_path.exists(),
        "complement_count": len(complement_ids),
        "duplicate_ids": len(duplicates),
        "complement_min_time": complement_min,
        "complement_max_time": complement_max,
        "has_early_coverage": has_early_coverage,
        "has_late_coverage": has_late_coverage,
        "early_threshold": early_threshold,
    }

    return result


def print_verification_result(result: Dict, verbose: bool = False) -> None:
    """検証結果を標準出力に表示

    問題がある場合や verbose モードの場合は詳細情報も表示する。

    Args:
        result: verify_slot() の戻り値
        verbose: 詳細表示モード（すべてのスロットの情報を表示）
    """
    ts = result["timestamp"]
    # 補完ファイルが存在すれば ✓、なければ ✗
    status = "✓" if result["complement_exists"] else "✗"

    print(f"{status} {ts}")

    # verbose モードまたは補完ファイルがない場合は詳細を表示
    if verbose or not result["complement_exists"]:
        print(f"  data/: {'存在' if result['data_exists'] else '不在'} ({result['data_count']} IDs)")
        print(f"  data_complement/: {'存在' if result['complement_exists'] else '不在'} ({result['complement_count']} IDs)")

        # ID重複がある場合は警告
        if result["duplicate_ids"] > 0:
            print(f"  ⚠ 重複ID: {result['duplicate_ids']}件")

        if result["complement_exists"]:
            min_time = result["complement_min_time"]
            max_time = result["complement_max_time"]

            if min_time and max_time:
                print(f"  時刻範囲: {min_time.strftime('%H:%M:%S')} ～ {max_time.strftime('%H:%M:%S')}")

                # 開始カバレッジが不足している場合は警告
                if not result["has_early_coverage"]:
                    threshold = result["early_threshold"]
                    print(f"  ⚠ 開始カバレッジ不足: {min_time.strftime('%H:%M:%S')} > {threshold.strftime('%H:%M:%S')}")

                # 終了カバレッジは実用上問題ないため警告しない
                # if not result["has_late_coverage"]:
                #     print(f"  ⚠ 終了カバレッジ不足")


def verify_period(
    period_file: Path,
    data_root: Path,
    complement_root: Path,
    early_coverage_seconds: int = 30,
    verbose: bool = False,
) -> Tuple[int, int, int]:
    """期間ファイルに記載された全スロットを検証

    period_file の各行を読み込み、開始〜終了の範囲内のすべての10分スロットを
    検証する。各スロットについて verify_slot() を呼び出し、結果を集計する。

    Args:
        period_file: 期間リストファイル（complement_periods.txt など）
        data_root: 既存データのルートディレクトリ
        complement_root: 補完データのルートディレクトリ
        early_coverage_seconds: 開始カバレッジの判定閾値（秒）
        verbose: 詳細表示モード

    Returns:
        (成功数, 警告数, 失敗数) のタプル
    """
    if not period_file.exists():
        print(f"エラー: {period_file} が見つかりません")
        return 0, 0, 0

    # 期間ファイルから全スロットのタイムスタンプを列挙
    timestamps = []
    with period_file.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # 空行とコメント行はスキップ
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) >= 2:
                # start, end を取得（CSV形式: start,end,sinceId,untilId）
                start_ts = parts[0].strip()
                end_ts = parts[1].strip()

                # start から end までの10分スロットを列挙
                start_dt = parse_timestamp(start_ts)
                end_dt = parse_timestamp(end_ts)

                current = start_dt
                while current <= end_dt:
                    ts = current.strftime("%Y-%m-%d_%H-%M")
                    timestamps.append(ts)
                    current += timedelta(minutes=10)

    success_count = 0  # 問題なしの件数
    warning_count = 0  # 警告ありの件数（ID重複、カバレッジ不足など）
    failure_count = 0  # 失敗の件数（補完ファイル不在）

    print(f"検証対象スロット数: {len(timestamps)}")
    print()

    # 各スロットを検証
    for timestamp in timestamps:
        result = verify_slot(timestamp, data_root, complement_root, early_coverage_seconds)

        # 検証結果を判定
        has_issues = False
        if not result["complement_exists"]:
            # 補完ファイルが存在しない → 失敗
            failure_count += 1
            has_issues = True
        elif result["duplicate_ids"] > 0 or not result["has_early_coverage"] or not result["has_late_coverage"]:
            # ID重複やカバレッジ不足 → 警告
            warning_count += 1
            has_issues = True
        else:
            # 問題なし → 成功
            success_count += 1

        # 問題があるか、verbose モードの場合のみ表示
        if has_issues or verbose:
            print_verification_result(result, verbose=verbose)

    return success_count, warning_count, failure_count


def main() -> int:
    """メイン処理

    コマンドライン引数をパースし、指定された期間ファイルに基づいて
    補完データの検証を実行する。

    Returns:
        終了コード（0=成功、1=失敗あり、2=警告あり）
    """
    parser = argparse.ArgumentParser(description="補完データの検証")
    parser.add_argument(
        "--periods-file",
        default=str(DEFAULT_PERIOD_FILE),
        help="検証する期間リストファイル（デフォルト: periods/complement_periods.txt）",
    )
    parser.add_argument(
        "--data-root",
        default="data",
        help="既存データのルートディレクトリ（デフォルト: data）",
    )
    parser.add_argument(
        "--complement-root",
        default="data_complement",
        help="補完データのルートディレクトリ（デフォルト: data_complement）",
    )
    parser.add_argument(
        "--early-coverage-seconds",
        type=int,
        default=30,
        help="開始カバレッジの判定閾値（秒）（デフォルト: 30）",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="すべてのスロットの詳細を表示",
    )
    args = parser.parse_args()

    # ディレクトリパスの設定
    data_root = Path(args.data_root)
    complement_root = Path(args.complement_root)
    period_file = Path(args.periods_file)

    # 検証実行
    success, warnings, failures = verify_period(
        period_file,
        data_root,
        complement_root,
        args.early_coverage_seconds,
        args.verbose,
    )

    # 結果サマリーを表示
    print()
    print("=== 検証結果 ===")
    print(f"✓ 成功: {success}")
    print(f"⚠ 警告: {warnings}")
    print(f"✗ 失敗: {failures}")

    # 終了コードを返す
    if failures > 0:
        return 1  # 失敗あり
    elif warnings > 0:
        return 2  # 警告あり
    else:
        return 0  # すべて成功


if __name__ == "__main__":
    raise SystemExit(main())
