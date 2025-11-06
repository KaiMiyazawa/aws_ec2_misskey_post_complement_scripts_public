#!/usr/bin/env python3
"""補完後の 10 分スロットの状態を確認するユーティリティ。

指定したスロットに対応する `data/` および `data_complement/` のファイルを読み込み、
以下を確認・出力する。

- ファイルごとの行数、ユニーク ID 数、重複件数
- `createdAt` の最小・最大（JST）
- スロット全体（10 分）で見た場合の統合件数、欠損の有無
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

JST = timezone(timedelta(hours=9))


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Misskey補完データのチェックツール")
    parser.add_argument("timestamp", help="確認するスロット (例: 2025-07-16_12-10)")
    parser.add_argument("--slot-minutes", type=int, default=10, help="スロット幅（分）")
    parser.add_argument("--data-root", default="data", help="既存データのルート")
    parser.add_argument(
        "--complement-root",
        default="data_complement",
        help="補完データのルート",
    )
    parser.add_argument(
        "--show-empty",
        action="store_true",
        help="ファイルが存在しない場合も結果に表示する",
    )
    return parser.parse_args()


def parse_timestamp(ts: str) -> datetime:
    try:
        dt = datetime.strptime(ts, "%Y-%m-%d_%H-%M")
    except ValueError as exc:
        raise SystemExit(f"timestamp の形式が不正です: {ts}") from exc
    return dt.replace(tzinfo=JST)


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


def collect_slot_stats(timestamp: str, slot_minutes: int, roots: Iterable[Path]) -> SlotStats:
    slot_start = parse_timestamp(timestamp)
    slot_end = slot_start + timedelta(minutes=slot_minutes)

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


def print_stats(stats: SlotStats, show_empty: bool) -> None:
    slot_label = stats.slot_start.strftime("%Y-%m-%d %H:%M")
    print(f"スロット: {slot_label} ～ {(stats.slot_end - timedelta(minutes=1)).strftime('%H:%M')}")
    print(f"想定範囲: {stats.slot_start.strftime('%Y-%m-%d %H:%M:%S')} ～ {stats.slot_end.strftime('%Y-%m-%d %H:%M:%S')} (JST)")

    if stats.files:
        for f in stats.files:
            print(f"- {f.path}")
            print(f"    行数: {f.lines}")
            print(f"    ユニークID: {f.unique_ids} (重複 {f.duplicate_ids})")
            if f.min_dt:
                print(f"    createdAt 最小: {f.min_dt}")
            if f.max_dt:
                print(f"    createdAt 最大: {f.max_dt}")
    elif show_empty:
        print("- 対象ファイルは存在しません。")

    print("--- まとめ ---")
    print(f"統合行数: {stats.total_lines}")
    print(f"統合ユニークID: {stats.total_unique_ids} (重複 {stats.total_duplicates})")
    if stats.min_dt:
        print(f"統合 createdAt 最小: {stats.min_dt}")
    else:
        print("統合 createdAt 最小: 取得なし")
    if stats.max_dt:
        print(f"統合 createdAt 最大: {stats.max_dt}")
    else:
        print("統合 createdAt 最大: 取得なし")

    if stats.min_dt and stats.max_dt:
        cover_start = stats.min_dt <= stats.slot_start
        cover_end = stats.max_dt >= stats.slot_end - timedelta(seconds=1)
        print(f"カバレッジ: 開始 {'OK' if cover_start else '不足'} / 終了 {'OK' if cover_end else '不足'}")
    else:
        print("カバレッジ: データ不足")


def main() -> int:
    args = parse_args()
    roots = [Path(args.data_root), Path(args.complement_root)]
    stats = collect_slot_stats(args.timestamp, args.slot_minutes, roots)
    print_stats(stats, args.show_empty)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
