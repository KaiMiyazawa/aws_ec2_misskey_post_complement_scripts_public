#!/usr/bin/env python3
"""欠損補完とカバレッジ確認をまとめて実行するラッパー。

基本的に README に記載の推奨パラメータ（timeline + sub-slot 60 秒）を用いて
`scripts/pipeline/complement_missing.py` を呼び出し、その後 `scripts/checks/check_slot_coverage.py`
で結果を表示する。
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPLEMENT_SCRIPT = REPO_ROOT / "scripts" / "pipeline" / "complement_missing.py"
CHECK_SCRIPT = REPO_ROOT / "scripts" / "checks" / "check_slot_coverage.py"

DEFAULT_COMPLEMENT_ARGS = [
    "--mode",
    "timeline",
    "--endpoint",
    "notes/global-timeline",
    "--limit",
    "100",
    "--sub-slot-seconds",
    "30",
    "--sleep",
    "5",
    "--overwrite",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="補完＋カバレッジ確認のラッパー")
    parser.add_argument("timestamp", help="対象スロット (例: 2025-07-16_12-10)")
    parser.add_argument(
        "--use-search",
        action="store_true",
        help="notes/search モードで補完（内部的には complement_missing_search.py を利用）",
    )
    parser.add_argument(
        "--sub-slot-seconds",
        type=int,
        default=60,
        help="timeline モード時のサブスロット幅（秒）",
    )
    parser.add_argument(
        "--token",
        help="Misskey APIトークン。未指定なら環境変数 MISSKEY_TOKEN を利用",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="補完コマンドだけ表示し実行しない",
    )
    return parser.parse_args()


def slot_to_iso(timestamp: str) -> str:
    if "_" in timestamp:
        date_part, time_part = timestamp.split("_", 1)
        time_part = time_part.replace("-", ":")
        return f"{date_part}T{time_part}"
    return timestamp


def build_timeline_command(timestamp: str, sub_slot_seconds: int, token: str) -> list[str]:
    iso = slot_to_iso(timestamp)
    cmd = [
        sys.executable,
        str(COMPLEMENT_SCRIPT),
        "--start",
        iso,
        "--end",
        iso,
    ]
    base_index = len(cmd)
    cmd.extend(DEFAULT_COMPLEMENT_ARGS)
    sub_offset = DEFAULT_COMPLEMENT_ARGS.index("--sub-slot-seconds") + 1
    cmd[base_index + sub_offset] = str(sub_slot_seconds)
    cmd.extend(["--token", token])
    return cmd


def build_search_command(timestamp: str, sub_slot_seconds: int, token: str) -> list[str]:
    script = REPO_ROOT / "scripts" / "pipeline" / "complement_missing_search.py"
    iso = slot_to_iso(timestamp)
    return [
        sys.executable,
        str(script),
        "--start",
        iso,
        "--end",
        iso,
        "--sub-slot-seconds",
        str(sub_slot_seconds),
        "--token",
        token,
    ]


def run_command(cmd: list[str], dry_run: bool) -> int:
    if dry_run:
        print("Command:", " ".join(cmd))
        return 0
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] コマンド失敗: {exc.returncode}", file=sys.stderr)
        return exc.returncode
    return 0


def main() -> int:
    args = parse_args()
    token = args.token or os.environ.get("MISSKEY_TOKEN")
    if not token:
        print("[ERROR] APIトークンを --token または環境変数 MISSKEY_TOKEN で指定してください。", file=sys.stderr)
        return 1

    if args.use_search:
        complement_cmd = build_search_command(args.timestamp, args.sub_slot_seconds, token)
    else:
        complement_cmd = build_timeline_command(args.timestamp, args.sub_slot_seconds, token)

    ret = run_command(complement_cmd, args.dry_run)
    if ret != 0 or args.dry_run:
        return ret

    check_cmd = [
        sys.executable,
        str(CHECK_SCRIPT),
        args.timestamp,
    ]
    print("\n=== カバレッジ確認 ===")
    run_command(check_cmd, dry_run=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
