#!/usr/bin/env python3
"""complement_periods.txtの全期間に対して補完を一括実行するバッチスクリプト

このスクリプトは complement_periods.txt に記載された全期間について、
complement_missing.py を順次実行し、データ補完を自動化する。

進捗管理機能:
- 進捗ファイル (.complement_progress.json) で各期間の状態を記録
- Ctrl+C で中断しても進捗は保存される
- --resume オプションで完了済み期間をスキップして再開可能

実行される complement_missing.py の設定:
- --mode search (notes/search API を使用)
- --limit 100 (1回のAPI呼び出しで最大100件取得)
- --max-pages 100 (最大100ページまでページネーション)
- --sleep 5 (API呼び出し間隔: 5秒)
- --overwrite (既存ファイルを上書き)
- --keep-non-japanese (日本語フィルタを無効化)
- --early-coverage-seconds (デフォルト30秒、変更可能)

使用例:
    # 基本的な使い方（進捗管理あり、推奨）
    python scripts/pipeline/run_complement_batch.py \
        --token "$MISSKEY_TOKEN" \
        --resume

    # レートリミットで中断された場合の再開
    # Ctrl+C で中断
    ^C
    # しばらく待ってから再実行（完了済みはスキップされる）
    python scripts/pipeline/run_complement_batch.py \
        --token "$MISSKEY_TOKEN" \
        --resume

    # 進捗をクリアして最初から
    python scripts/pipeline/run_complement_batch.py \
        --token "$MISSKEY_TOKEN" \
        --clear-progress \
        --resume

    # dry-run モード（実際には実行せず、コマンドのみ表示）
    python scripts/pipeline/run_complement_batch.py --token "$MISSKEY_TOKEN" --dry-run

    # 途中から再開（10行目から）
    python scripts/pipeline/run_complement_batch.py --token "$MISSKEY_TOKEN" --start-from 10

    # 処理件数を制限（最初の5期間のみ）
    python scripts/pipeline/run_complement_batch.py --token "$MISSKEY_TOKEN" --limit 5 --resume
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# 日本標準時（JST = UTC+9）のタイムゾーン定義
JST = timezone(timedelta(hours=9))

# スクリプトの配置場所を基準としたパス設定
REPO_ROOT = Path(__file__).resolve().parents[2]
COMPLEMENT_SCRIPT = REPO_ROOT / "scripts" / "pipeline" / "complement_missing.py"  # 補完スクリプト
PERIODS_FILE = REPO_ROOT / "periods" / "complement_periods.txt"  # 期間リストファイル
DEFAULT_PROGRESS_FILE = REPO_ROOT / ".complement_progress.json"  # 進捗ファイル


class ProgressTracker:
    """補完処理の進捗を管理するクラス

    進捗ファイル（.complement_progress.json）に各期間の状態を記録し、
    再実行時に完了済み期間をスキップできるようにする。

    状態の種類:
    - pending: 未実行
    - in_progress: 実行中（中断された可能性あり）
    - completed: 完了
    - failed: 失敗
    """

    def __init__(self, progress_file: Path):
        """ProgressTrackerの初期化

        Args:
            progress_file: 進捗ファイルのパス
        """
        self.progress_file = progress_file
        self.data = self.load()

    def load(self) -> Dict:
        """進捗ファイルを読み込む

        Returns:
            進捗データの辞書（ファイルが存在しない場合は空の辞書）
        """
        if self.progress_file.exists():
            try:
                with open(self.progress_file, encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                # ファイルが壊れている場合は空から開始
                return {"last_updated": None, "periods": {}}
        return {"last_updated": None, "periods": {}}

    def save(self) -> None:
        """進捗ファイルに保存

        保存前に last_updated を現在時刻に更新する。
        """
        self.data["last_updated"] = datetime.now(JST).isoformat()
        with open(self.progress_file, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def get_status(self, period_key: str) -> str:
        """指定期間の状態を取得

        Args:
            period_key: 期間を識別するキー（例: "2025-08-01_00-40,2025-08-01_00-40,..."）

        Returns:
            状態文字列（"pending", "in_progress", "completed", "failed"）
        """
        return self.data["periods"].get(period_key, {}).get("status", "pending")

    def should_skip(self, period_key: str) -> bool:
        """指定期間をスキップすべきか判定

        completed 状態の期間のみスキップする。
        in_progress, failed, pending は再実行する。

        Args:
            period_key: 期間を識別するキー

        Returns:
            True: スキップする（completed）
            False: 実行する（それ以外）
        """
        status = self.get_status(period_key)
        return status == "completed"

    def mark_in_progress(self, period_key: str) -> None:
        """指定期間を「実行中」状態にする

        Args:
            period_key: 期間を識別するキー
        """
        if period_key not in self.data["periods"]:
            self.data["periods"][period_key] = {}

        self.data["periods"][period_key].update({
            "status": "in_progress",
            "started_at": datetime.now(JST).isoformat(),
        })
        self.save()

    def mark_completed(self, period_key: str) -> None:
        """指定期間を「完了」状態にする

        Args:
            period_key: 期間を識別するキー
        """
        if period_key not in self.data["periods"]:
            self.data["periods"][period_key] = {}

        self.data["periods"][period_key].update({
            "status": "completed",
            "completed_at": datetime.now(JST).isoformat(),
        })
        self.save()

    def mark_failed(self, period_key: str, error_message: str) -> None:
        """指定期間を「失敗」状態にする

        Args:
            period_key: 期間を識別するキー
            error_message: エラーメッセージ
        """
        if period_key not in self.data["periods"]:
            self.data["periods"][period_key] = {}

        self.data["periods"][period_key].update({
            "status": "failed",
            "failed_at": datetime.now(JST).isoformat(),
            "error_message": error_message,
        })
        self.save()

    def clear(self) -> None:
        """進捗ファイルをクリア（全期間をpending状態にリセット）"""
        self.data = {"last_updated": None, "periods": {}}
        self.save()

    def get_summary(self) -> Dict[str, int]:
        """進捗のサマリーを取得

        Returns:
            各状態の件数を含む辞書
        """
        summary = {
            "total": len(self.data["periods"]),
            "completed": 0,
            "in_progress": 0,
            "failed": 0,
            "pending": 0,
        }

        for period_data in self.data["periods"].values():
            status = period_data.get("status", "pending")
            if status in summary:
                summary[status] += 1

        return summary


def parse_args() -> argparse.Namespace:
    """コマンドライン引数をパース

    Returns:
        パースされた引数のNamespaceオブジェクト
    """
    parser = argparse.ArgumentParser(
        description="complement_periods.txtの全期間を一括補完"
    )
    parser.add_argument(
        "--token",
        help="Misskey API トークン。未指定なら環境変数 MISSKEY_TOKEN を利用",
    )
    # sub-slot-secondsは削除（offsetベースのページネーションに移行）
    # parser.add_argument(
    #     "--sub-slot-seconds",
    #     type=int,
    #     help="スロットをこの秒数で細分化して取得 (例: 60で1分刻み)",
    # )
    parser.add_argument(
        "--sleep",
        type=float,
        default=5.0,
        help="API呼び出し間隔（秒）。デフォルト: 5.0",
    )
    parser.add_argument(
        "--period-sleep",
        type=float,
        default=5.0,
        help="各期間の補完後の待機時間（秒）。デフォルト: 5.0",
    )
    parser.add_argument(
        "--early-coverage-seconds",
        type=int,
        default=30,
        help="期間開始から何秒以内のノートが取得できたら完了とするか（デフォルト: 30秒）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="実際には取得せず実行コマンドを表示",
    )
    parser.add_argument(
        "--periods-file",
        default=str(PERIODS_FILE),
        help=f"補完期間リストファイル (デフォルト: {PERIODS_FILE})",
    )
    parser.add_argument(
        "--start-from",
        type=int,
        default=1,
        help="何行目から開始するか (デフォルト: 1)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="処理する期間数の上限 (未指定なら全期間)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="進捗ファイルを参照して、完了済み期間をスキップ",
    )
    parser.add_argument(
        "--progress-file",
        default=str(DEFAULT_PROGRESS_FILE),
        help=f"進捗ファイルのパス（デフォルト: {DEFAULT_PROGRESS_FILE.name}）",
    )
    parser.add_argument(
        "--clear-progress",
        action="store_true",
        help="進捗ファイルをクリアして最初から実行",
    )
    return parser.parse_args()


def make_period_key(period: Tuple[str, str, str, str]) -> str:
    """期間タプルから一意なキーを生成

    Args:
        period: (start, end, since_id, until_id) のタプル

    Returns:
        "start,end,since_id,until_id" 形式のキー文字列
    """
    start, end, since_id, until_id = period
    return f"{start},{end},{since_id},{until_id}"


def load_periods(periods_file: Path) -> List[Tuple[str, str, str, str]]:
    """complement_periods.txtを読み込んで期間リストを返す

    ファイルの各行は CSV 形式: start,end,sinceId,untilId
    空行とコメント行（#で始まる行）はスキップされる。

    Args:
        periods_file: 期間リストファイルのパス

    Returns:
        [(start_timestamp, end_timestamp, since_id, until_id), ...] のリスト
        例: [("2025-08-01_00-40", "2025-08-01_00-40", "abc123", "def456"), ...]
    """
    if not periods_file.exists():
        print(f"[ERROR] {periods_file} が見つかりません", file=sys.stderr)
        sys.exit(1)

    periods = []
    with periods_file.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) != 4:
                print(f"[WARNING] 形式が不正な行をスキップ (期待: start,end,sinceId,untilId): {line}", file=sys.stderr)
                continue
            start, end, since_id, until_id = [p.strip() for p in parts]
            periods.append((start, end, since_id, until_id))
    return periods


def timestamp_to_datetime_str(timestamp: str) -> str:
    """YYYY-MM-DD_HH-MM を YYYY-MM-DDTHH:MM に変換

    complement_missing.py の --start/--end 引数に渡すため、
    タイムスタンプ形式を変換する。

    Args:
        timestamp: タイムスタンプ文字列（例: "2025-08-01_00-40"）

    Returns:
        ISO8601形式の日時文字列（例: "2025-08-01T00:40"）

    例:
        2025-08-01_00-40 -> 2025-08-01T00:40
    """
    # YYYY-MM-DD_HH-MM を分割
    date_part, time_part = timestamp.split("_")
    # 時刻部分の - を : に変換
    time_part = time_part.replace("-", ":")
    # 結合
    return f"{date_part}T{time_part}"


def run_complement(
    start: str,
    end: str,
    since_id: str,
    until_id: str,
    token: str,
    sleep: float,
    early_coverage_seconds: int,
    dry_run: bool,
) -> int:
    """complement_missing.pyを実行

    指定された期間について、complement_missing.py をサブプロセスとして実行する。
    推奨設定（--mode search, --limit 100, etc.）で自動的に実行される。

    Args:
        start: 開始タイムスタンプ（YYYY-MM-DD_HH-MM形式）
        end: 終了タイムスタンプ（YYYY-MM-DD_HH-MM形式）
        since_id: 取得範囲の開始ID
        until_id: 取得範囲の終了ID
        token: Misskey APIトークン
        sleep: API呼び出し間隔（秒）
        early_coverage_seconds: 開始カバレッジの判定閾値（秒）
        dry_run: dry-runモード（コマンドのみ表示）

    Returns:
        終了コード（0=成功、それ以外=失敗）
    """
    # タイムスタンプをISO8601形式に変換
    start_dt = timestamp_to_datetime_str(start)
    end_dt = timestamp_to_datetime_str(end)

    # complement_missing.py に渡すコマンドを構築
    cmd = [
        sys.executable,  # Pythonインタプリタ
        str(COMPLEMENT_SCRIPT),  # complement_missing.py のパス
        "--start",
        start_dt,
        "--end",
        end_dt,
        "--mode",
        "search",  # notes/search を使用
        "--limit",
        "100",  # 1回のAPI呼び出しで最大100件
        "--max-pages",
        "1000000",  # 最大100ページまでページネーション
        "--sleep",
        str(sleep),  # API呼び出し間隔
        "--overwrite",  # 既存ファイルを上書き
        "--keep-non-japanese",  # 日本語フィルタを無効化
        "--early-coverage-seconds",
        str(early_coverage_seconds),  # 開始カバレッジの判定閾値
        "--token",
        token,  # APIトークン
    ]

    # sinceId/untilIdを追加（指定されている場合）
    if since_id:
        cmd.extend(["--since-id", since_id])
    if until_id:
        cmd.extend(["--until-id", until_id])

    # dry-run モードの場合はコマンドのみ表示
    if dry_run:
        print(f"[DRY-RUN] {' '.join(cmd)}")
        return 0

    # サブプロセスとして実行
    try:
        subprocess.run(cmd, check=True)
        return 0
    except subprocess.CalledProcessError as exc:
        print(
            f"[ERROR] 補完コマンドが失敗しました (exit={exc.returncode})",
            file=sys.stderr,
        )
        return exc.returncode


def main() -> int:
    """メイン処理

    コマンドライン引数をパースし、期間リストファイルを読み込んで、
    各期間について順次 complement_missing.py を実行する。
    --resume オプション使用時は進捗ファイルで完了済み期間をスキップ。

    Returns:
        終了コード（0=すべて成功、1=1件以上失敗）
    """
    # コマンドライン引数をパース
    args = parse_args()

    # APIトークンの取得（引数または環境変数）
    token = args.token or os.environ.get("MISSKEY_TOKEN")
    if not token:
        print(
            "[ERROR] APIトークンを --token または環境変数 MISSKEY_TOKEN で指定してください。",
            file=sys.stderr,
        )
        return 1

    # 進捗トラッカーの初期化
    progress_file = Path(args.progress_file)
    progress = ProgressTracker(progress_file)

    # --clear-progress が指定されている場合は進捗をクリア
    if args.clear_progress:
        print(f"進捗ファイルをクリアしました: {progress_file}")
        progress.clear()

    # 期間リストファイルを読み込み
    periods_file = Path(args.periods_file)
    periods = load_periods(periods_file)

    if not periods:
        print("[ERROR] 補完すべき期間が見つかりませんでした", file=sys.stderr)
        return 1

    # --start-from と --limit を適用して処理対象期間を選択
    start_idx = args.start_from - 1  # 1-indexed -> 0-indexed
    if start_idx < 0:
        start_idx = 0
    if start_idx >= len(periods):
        print(
            f"[ERROR] --start-from {args.start_from} は範囲外です (全{len(periods)}期間)",
            file=sys.stderr,
        )
        return 1

    end_idx = len(periods)
    if args.limit:
        # 処理件数を制限
        end_idx = min(start_idx + args.limit, len(periods))

    selected_periods = periods[start_idx:end_idx]

    # 実行情報を表示
    print(f"=== 補完バッチ実行 ===")
    print(f"期間ファイル: {periods_file}")
    print(f"進捗ファイル: {progress_file}")
    print(f"レジュームモード: {'有効' if args.resume else '無効'}")
    print(f"全期間数: {len(periods)}")
    print(f"処理対象: {len(selected_periods)} 期間 (#{start_idx+1} ～ #{end_idx})")

    # 進捗サマリーを表示（--resume モードの場合）
    if args.resume:
        summary = progress.get_summary()
        print(f"進捗状況: 完了={summary['completed']}, 実行中={summary['in_progress']}, "
              f"失敗={summary['failed']}, 未実行={len(periods) - summary['total']}")
    print()

    success_count = 0  # 成功した期間数
    failure_count = 0  # 失敗した期間数
    skipped_count = 0  # スキップした期間数

    # 各期間について補完を実行
    try:
        for idx, period in enumerate(selected_periods, start=start_idx + 1):
            start, end, since_id, until_id = period
            period_key = make_period_key(period)
            status = progress.get_status(period_key)

            # --resume モードで completed の場合はスキップ
            if args.resume and progress.should_skip(period_key):
                print(f"[{idx}/{len(periods)}] ✓ スキップ: {start} ～ {end} (completed)")
                skipped_count += 1
                success_count += 1  # スキップも成功としてカウント
                continue

            # 状態に応じたメッセージ表示
            id_info = f"sinceId={since_id or 'N/A'}, untilId={until_id or 'N/A'}"
            if status == "in_progress":
                print(f"[{idx}/{len(periods)}] ⟳ 再実行: {start} ～ {end} (was in_progress, {id_info})")
            elif status == "failed":
                print(f"[{idx}/{len(periods)}] ⚠ 再実行: {start} ～ {end} (was failed, {id_info})")
            else:
                print(f"[{idx}/{len(periods)}] 補完中: {start} ～ {end} ({id_info})")

            # 進捗を in_progress に更新
            if args.resume:
                progress.mark_in_progress(period_key)

            # complement_missing.py を実行
            ret = run_complement(
                start,
                end,
                since_id,
                until_id,
                token,
                args.sleep,
                args.early_coverage_seconds,
                args.dry_run,
            )

            # 結果を集計
            if ret == 0:
                success_count += 1
                # 進捗を completed に更新
                if args.resume:
                    progress.mark_completed(period_key)
            else:
                failure_count += 1
                print(f"[{idx}/{len(periods)}] 失敗: {start} ～ {end}", file=sys.stderr)
                # 進捗を failed に更新
                if args.resume:
                    progress.mark_failed(period_key, f"補完スクリプトが exit={ret} で終了")

            # 期間間の待機（API負荷軽減のため）
            if args.period_sleep > 0 and idx < len(periods):
                import time
                time.sleep(args.period_sleep)

    except KeyboardInterrupt:
        # Ctrl+C で中断
        print()
        print("=== 中断されました ===")
        print("進捗は保存されています。--resume オプションで再開できます。")
        return 130  # 128 + SIGINT(2)

    # 結果サマリーを表示
    print()
    print("=== 実行結果 ===")
    print(f"成功: {success_count}")
    if skipped_count > 0:
        print(f"スキップ: {skipped_count} (既に完了)")
    print(f"失敗: {failure_count}")

    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
