#!/usr/bin/env python3
"""指定期間の全スロットのカバレッジを一括チェックするツール。

2025-08-01 から 2025-10-09 のような期間を指定すると、
その間の全10分スロットに対して check_slot_coverage.py を実行し、
カバレッジ不足のスロットをレポートする。
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

JST = timezone(timedelta(hours=9))


# ============================================================
# 欠損判定の設定（マクロ）
# ============================================================

# 【データ有無の判定基準】
# ユニークID数がこの値以下の場合、「データなし」として扱う
MIN_UNIQUE_IDS_THRESHOLD = 100  # デフォルト: 0 (1件以上あればデータありとみなす)
                               # 例: 100 に設定すると、100ID以下は「データなし」扱い

# 【カバレッジ判定の許容時間（秒）】
# スロット開始時刻から何秒以内にデータがあれば「開始をカバー」とみなすか
COVERAGE_START_TOLERANCE_SECONDS = 10

# スロット終了時刻から何秒以内にデータがあれば「終了をカバー」とみなすか
COVERAGE_END_TOLERANCE_SECONDS = 10

# 【欠損出力モード】
# True: ID数が0の時とデータなしの時のみ出力（不完全だがデータがあるものは除外）
# False: 不完全なスロットとデータなしの両方を出力
ONLY_OUTPUT_NO_DATA_OR_ZERO_IDS = True

# ============================================================
# 使用例:
#
# 1. 100ID以下を「データなし」として扱う:
#    MIN_UNIQUE_IDS_THRESHOLD = 100
#
# 2. より厳密にカバレッジを判定（開始5秒、終了5秒以内）:
#    COVERAGE_START_TOLERANCE_SECONDS = 5
#    COVERAGE_END_TOLERANCE_SECONDS = 5
#
# 3. 完全にデータがないスロットのみを補完対象にする:
#    ONLY_OUTPUT_NO_DATA_OR_ZERO_IDS = True
#    MIN_UNIQUE_IDS_THRESHOLD = 0
#
# 4. 少量データ（50ID以下）を欠損扱いにし、不完全なものは除外:
#    MIN_UNIQUE_IDS_THRESHOLD = 50
#    ONLY_OUTPUT_NO_DATA_OR_ZERO_IDS = True
# ============================================================


@dataclass
class FileStats:
    path: Path
    lines: int
    unique_ids: int
    duplicate_ids: int
    min_dt: Optional[datetime]
    max_dt: Optional[datetime]


@dataclass
class SlotStats:
    slot_start: datetime
    slot_end: datetime
    files: List[FileStats]
    total_lines: int
    total_unique_ids: int
    total_duplicates: int
    min_dt: Optional[datetime]
    max_dt: Optional[datetime]


@dataclass
class CoverageResult:
    timestamp: str
    slot_start: datetime
    slot_end: datetime
    total_lines: int
    total_unique_ids: int
    has_data: bool
    cover_start: bool
    cover_end: bool
    is_complete: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="期間指定でカバレッジを一括チェック")
    parser.add_argument("--start", required=True, help="開始日 (例: 2025-08-01)")
    parser.add_argument("--end", required=True, help="終了日 (例: 2025-10-09)")
    parser.add_argument("--slot-minutes", type=int, default=10, help="スロット幅（分）")
    parser.add_argument("--data-root", default="data", help="既存データのルート")
    parser.add_argument(
        "--complement-root",
        default="data_complement",
        help="補完データのルート",
    )
    parser.add_argument(
        "--show-complete",
        action="store_true",
        help="完全なスロットも表示する",
    )
    parser.add_argument(
        "--output-missing",
        help="欠損スロットのタイムスタンプをファイルに出力",
    )
    parser.add_argument(
        "--output-periods",
        help="補完すべき期間を連続した範囲としてファイルに出力",
    )
    return parser.parse_args()


def parse_date(date_str: str) -> datetime:
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"日付の形式が不正です: {date_str}") from exc
    return dt.replace(tzinfo=JST)


def get_boundary_ids(path: Path) -> tuple[Optional[str], Optional[str]]:
    """ファイルの最初と最後のノートIDを取得

    Returns:
        (first_id, last_id): 最初のID、最後のID
    """
    if not path.exists():
        return None, None

    first_id = None
    last_id = None

    with path.open(encoding="utf-8") as f:
        for raw in f:
            try:
                note = json.loads(raw)
                note_id = note.get("id")
                if note_id:
                    if first_id is None:
                        first_id = note_id
                    last_id = note_id
            except json.JSONDecodeError:
                continue

    return first_id, last_id


def load_file_stats(path: Path) -> Optional[FileStats]:
    if not path.exists():
        return None

    lines = 0
    ids: Dict[str, int] = {}
    min_dt: Optional[datetime] = None
    max_dt: Optional[datetime] = None

    with path.open(encoding="utf-8") as f:
        for raw in f:
            lines += 1
            try:
                note = json.loads(raw)
            except json.JSONDecodeError:
                continue
            note_id = note.get("id")
            if note_id:
                ids[note_id] = ids.get(note_id, 0) + 1
            created_str = note.get("createdAt")
            if created_str:
                dt = datetime.fromisoformat(created_str.replace("Z", "+00:00")).astimezone(JST)
                if min_dt is None or dt < min_dt:
                    min_dt = dt
                if max_dt is None or dt > max_dt:
                    max_dt = dt

    unique_ids = len(ids)
    duplicate_ids = sum(1 for cnt in ids.values() if cnt > 1)
    return FileStats(path, lines, unique_ids, duplicate_ids, min_dt, max_dt)


def collect_slot_stats(slot_start: datetime, slot_minutes: int, roots: List[Path]) -> SlotStats:
    slot_end = slot_start + timedelta(minutes=slot_minutes)
    timestamp = slot_start.strftime("%Y-%m-%d_%H-%M")

    files: List[FileStats] = []
    for root in roots:
        path = root.joinpath(
            slot_start.strftime("%Y"),
            slot_start.strftime("%m"),
            slot_start.strftime("%d"),
            slot_start.strftime("%H"),
            f"{timestamp}.jsonl",
        )
        stats = load_file_stats(path)
        if stats:
            files.append(stats)

    total_lines = sum(f.lines for f in files)
    total_unique_ids = sum(f.unique_ids for f in files)
    total_duplicates = sum(f.duplicate_ids for f in files)

    min_dt = None
    max_dt = None
    for f in files:
        if f.min_dt and (min_dt is None or f.min_dt < min_dt):
            min_dt = f.min_dt
        if f.max_dt and (max_dt is None or f.max_dt > max_dt):
            max_dt = f.max_dt

    return SlotStats(
        slot_start=slot_start,
        slot_end=slot_end,
        files=files,
        total_lines=total_lines,
        total_unique_ids=total_unique_ids,
        total_duplicates=total_duplicates,
        min_dt=min_dt,
        max_dt=max_dt,
    )


def check_coverage(stats: SlotStats) -> CoverageResult:
    timestamp = stats.slot_start.strftime("%Y-%m-%d_%H-%M")

    # MIN_UNIQUE_IDS_THRESHOLD を使ってデータ有無を判定
    has_data = stats.total_unique_ids > MIN_UNIQUE_IDS_THRESHOLD

    if stats.min_dt and stats.max_dt and has_data:
        # カバレッジ判定の許容時間をマクロから取得
        cover_start = stats.min_dt <= stats.slot_start + timedelta(seconds=COVERAGE_START_TOLERANCE_SECONDS)
        cover_end = stats.max_dt >= stats.slot_end - timedelta(seconds=COVERAGE_END_TOLERANCE_SECONDS)
        is_complete = cover_start and cover_end
    else:
        cover_start = False
        cover_end = False
        is_complete = False

    return CoverageResult(
        timestamp=timestamp,
        slot_start=stats.slot_start,
        slot_end=stats.slot_end,
        total_lines=stats.total_lines,
        total_unique_ids=stats.total_unique_ids,
        has_data=has_data,
        cover_start=cover_start,
        cover_end=cover_end,
        is_complete=is_complete,
    )


def generate_slots(start_date: datetime, end_date: datetime, slot_minutes: int) -> List[datetime]:
    slots = []
    current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    while current <= end:
        slots.append(current)
        current += timedelta(minutes=slot_minutes)

    return slots


def group_missing_periods(missing_slots: List[str], slot_minutes: int) -> List[tuple[str, str]]:
    """欠損スロットを連続した期間にグループ化する。

    Returns:
        [(start_timestamp, end_timestamp), ...] のリスト
    """
    if not missing_slots:
        return []

    periods = []
    current_start = None
    current_end = None

    for i, ts in enumerate(missing_slots):
        dt = datetime.strptime(ts, "%Y-%m-%d_%H-%M").replace(tzinfo=JST)

        if current_start is None:
            current_start = ts
            current_end = ts
        else:
            prev_dt = datetime.strptime(current_end, "%Y-%m-%d_%H-%M").replace(tzinfo=JST)
            expected_next = prev_dt + timedelta(minutes=slot_minutes)

            if dt == expected_next:
                # 連続している
                current_end = ts
            else:
                # 連続が途切れた
                periods.append((current_start, current_end))
                current_start = ts
                current_end = ts

    # 最後の期間を追加
    if current_start is not None:
        periods.append((current_start, current_end))

    return periods


def find_boundary_ids_for_period(
    start_ts: str,
    end_ts: str,
    slot_minutes: int,
    roots: List[Path]
) -> tuple[Optional[str], Optional[str]]:
    """欠損期間の前後のIDを探す

    Args:
        start_ts: 欠損期間の開始タイムスタンプ (例: "2025-08-01_00-40")
        end_ts: 欠損期間の終了タイムスタンプ
        slot_minutes: スロット幅（分）
        roots: データルートディレクトリのリスト

    Returns:
        (since_id, until_id): 直前のID、直後のID
    """
    start_dt = datetime.strptime(start_ts, "%Y-%m-%d_%H-%M").replace(tzinfo=JST)
    end_dt = datetime.strptime(end_ts, "%Y-%m-%d_%H-%M").replace(tzinfo=JST)

    # 直前のスロット（欠損期間の開始の1つ前）
    prev_slot = start_dt - timedelta(minutes=slot_minutes)
    prev_timestamp = prev_slot.strftime("%Y-%m-%d_%H-%M")

    # 直後のスロット（欠損期間の終了の1つ後）
    next_slot = end_dt + timedelta(minutes=slot_minutes)
    next_timestamp = next_slot.strftime("%Y-%m-%d_%H-%M")

    since_id = None
    until_id = None

    # 直前のスロットから最後のIDを取得
    for root in roots:
        path = root.joinpath(
            prev_slot.strftime("%Y"),
            prev_slot.strftime("%m"),
            prev_slot.strftime("%d"),
            prev_slot.strftime("%H"),
            f"{prev_timestamp}.jsonl",
        )
        _, last_id = get_boundary_ids(path)
        if last_id:
            since_id = last_id
            break

    # 直後のスロットから最初のIDを取得
    for root in roots:
        path = root.joinpath(
            next_slot.strftime("%Y"),
            next_slot.strftime("%m"),
            next_slot.strftime("%d"),
            next_slot.strftime("%H"),
            f"{next_timestamp}.jsonl",
        )
        first_id, _ = get_boundary_ids(path)
        if first_id:
            until_id = first_id
            break

    return since_id, until_id


def main() -> int:
    args = parse_args()
    start_date = parse_date(args.start)
    end_date = parse_date(args.end)

    if start_date > end_date:
        print("[ERROR] 開始日が終了日より後です", file=sys.stderr)
        return 1

    roots = [Path(args.data_root), Path(args.complement_root)]
    slots = generate_slots(start_date, end_date, args.slot_minutes)

    print(f"期間: {args.start} ～ {args.end}")
    print(f"対象スロット数: {len(slots)}")
    print()
    print("=== 欠損判定設定 ===")
    print(f"データなし判定閾値: ユニークID数 ≤ {MIN_UNIQUE_IDS_THRESHOLD}")
    print(f"開始カバレッジ許容: {COVERAGE_START_TOLERANCE_SECONDS}秒")
    print(f"終了カバレッジ許容: {COVERAGE_END_TOLERANCE_SECONDS}秒")
    print(f"出力モード: {'ID数0/データなしのみ' if ONLY_OUTPUT_NO_DATA_OR_ZERO_IDS else '全欠損を出力'}")
    print()

    complete_count = 0
    incomplete_count = 0
    no_data_count = 0
    missing_slots = []

    for slot_start in slots:
        stats = collect_slot_stats(slot_start, args.slot_minutes, roots)
        result = check_coverage(stats)

        if result.is_complete:
            complete_count += 1
            if args.show_complete:
                print(f"✓ {result.timestamp}: 完全 ({result.total_unique_ids} IDs)")
        elif result.has_data:
            incomplete_count += 1
            status = []
            if not result.cover_start:
                status.append("開始不足")
            if not result.cover_end:
                status.append("終了不足")
            print(f"⚠ {result.timestamp}: 不完全 ({', '.join(status)}) - {result.total_unique_ids} IDs")

            # ONLY_OUTPUT_NO_DATA_OR_ZERO_IDS が True の場合、不完全だがデータがあるものは出力しない
            if not ONLY_OUTPUT_NO_DATA_OR_ZERO_IDS:
                missing_slots.append(result.timestamp)
        else:
            no_data_count += 1
            print(f"✗ {result.timestamp}: データなし")
            missing_slots.append(result.timestamp)

    print()
    print("=== サマリー ===")
    print(f"完全: {complete_count}")
    print(f"不完全: {incomplete_count}")
    print(f"データなし: {no_data_count}")
    print(f"カバレッジ率: {complete_count / len(slots) * 100:.2f}%")

    if args.output_missing and missing_slots:
        output_path = Path(args.output_missing)
        with output_path.open("w", encoding="utf-8") as f:
            for ts in missing_slots:
                f.write(f"{ts}\n")
        print(f"\n欠損スロットを {output_path} に出力しました ({len(missing_slots)} 件)")

    if args.output_periods and missing_slots:
        periods = group_missing_periods(missing_slots, args.slot_minutes)
        output_path = Path(args.output_periods)
        with output_path.open("w", encoding="utf-8") as f:
            for start, end in periods:
                # 境界IDを取得
                since_id, until_id = find_boundary_ids_for_period(start, end, args.slot_minutes, roots)
                # フォーマット: start,end,sinceId,untilId
                f.write(f"{start},{end},{since_id or ''},{until_id or ''}\n")
        print(f"\n補完すべき期間を {output_path} に出力しました ({len(periods)} 期間)")
        print("期間リスト:")
        for start, end in periods:
            since_id, until_id = find_boundary_ids_for_period(start, end, args.slot_minutes, roots)
            if start == end:
                print(f"  - {start} (sinceId: {since_id or 'N/A'}, untilId: {until_id or 'N/A'})")
            else:
                print(f"  - {start} ～ {end} (sinceId: {since_id or 'N/A'}, untilId: {until_id or 'N/A'})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
