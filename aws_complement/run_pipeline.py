#!/usr/bin/env python3
"""AWS EC2 上で Misskey データの欠損確認→補完→検証を自動化するスクリプト。"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]

try:  # pragma: no cover - optional dependency
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

if __package__ is None:
    sys.path.insert(0, str(REPO_ROOT))
    from aws_complement.s3_inventory import BucketSource, S3SlotInventory, build_s3_client
else:  # pragma: no cover
    from .s3_inventory import BucketSource, S3SlotInventory, build_s3_client

JST = timezone(timedelta(hours=9))


def load_complement_module():
    """既存の complement_missing.py をモジュールとして読み込む。"""
    module_path = REPO_ROOT / "scripts" / "pipeline" / "complement_missing.py"
    spec = importlib.util.spec_from_file_location("jri_complement_missing", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[arg-type]
    return module


CM = load_complement_module()
Slot = CM.Slot
MisskeyClient = CM.MisskeyClient
iter_slots = CM.iter_slots
iter_sub_ranges = CM.iter_sub_ranges
filter_japanese_notes = CM.filter_japanese_notes
parse_note_datetime = CM.parse_note_datetime


def normalize_prefix(prefix: Optional[str]) -> str:
    if not prefix:
        return ""
    return prefix.strip("/")


def parse_jst(value: str) -> datetime:
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid datetime format: {value}") from exc
    return dt.replace(tzinfo=JST)


@dataclass
class SlotReport:
    slot: Slot
    s3_key: str
    note_count: int
    earliest: Optional[datetime]
    latest: Optional[datetime]
    coverage_ok: bool


class AWSComplementPipeline:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.logger = logging.getLogger("aws_complement")
        self.progress_enabled = bool(getattr(args, "progress", False) and tqdm is not None)
        if getattr(args, "progress", False) and tqdm is None:
            self.logger.warning("tqdm がインストールされていないため、プログレスバーを表示できません。`pip install tqdm` を実行してください。")
        token = args.token or os.environ.get("MISSKEY_TOKEN")
        if not token:
            raise SystemExit("Misskey API トークンを --token か環境変数 MISSKEY_TOKEN で指定してください。")

        self.s3_client = build_s3_client(region_name=args.aws_region, profile=args.aws_profile)
        self.sources: List[BucketSource] = []
        if args.dataset == "jp":
            if not args.primary_bucket:
                raise SystemExit("JP データセット用のバケット (--primary-bucket) を指定してください。")
            self.sources.append(BucketSource(args.primary_bucket, normalize_prefix(args.primary_prefix)))
        elif args.dataset == "en":
            if not args.backup_bucket:
                raise SystemExit("EN データセット用のバケット (--backup-bucket) を指定してください。")
            self.sources.append(BucketSource(args.backup_bucket, normalize_prefix(args.backup_prefix)))
        else:  # pragma: no cover - argparse choices guard
            raise SystemExit(f"Unknown dataset: {args.dataset}")

        self.dest_source = BucketSource(
            args.complement_bucket,
            normalize_prefix(args.complement_prefix),
        )

        self.inventory = S3SlotInventory(self.s3_client, self.sources)

        self.client = MisskeyClient(
            base_url=args.base_url,
            token=token,
            endpoint=args.endpoint,
            timeout=args.timeout,
            retry=args.retry,
            retry_wait=args.retry_wait,
        )

        self.slot_reports: List[SlotReport] = []

    def build_slots(self) -> List[Slot]:
        slots = list(iter_slots(self.args.start_dt, self.args.end_dt, self.args.slot_minutes))
        if self.args.max_slots:
            slots = slots[: self.args.max_slots]
        return slots

    def detect_missing_slots(self, slots: Sequence[Slot]) -> List[Slot]:
        missing: List[Slot] = []
        iterator = self._iter_with_progress(slots, f"Scanning slots ({self.args.dataset.upper()})")
        for slot in iterator:
            if not self.inventory.slot_exists(slot.start, slot.timestamp):
                missing.append(slot)
        return missing

    def _iter_with_progress(self, items: Sequence[Slot], desc: str) -> Iterable[Slot]:
        if not self.progress_enabled or not items:
            for item in items:
                yield item
            return
        if tqdm is None:
            for item in items:
                yield item
            return
        with tqdm(total=len(items), desc=desc, unit="slot") as bar:
            for item in items:
                yield item
                bar.update(1)

    def fetch_slot_notes(self, slot: Slot) -> List[dict]:
        seen_ids: set[str] = set()
        notes: List[dict] = []

        if self.args.sub_slot_seconds:
            delta = timedelta(seconds=self.args.sub_slot_seconds)
            for sub_start, sub_end in iter_sub_ranges(slot.start, slot.end, delta):
                sub_notes = self.client.fetch_notes(
                    mode=self.args.mode,
                    start=sub_start,
                    end=sub_end,
                    limit=self.args.limit,
                    query=self.args.query,
                    host=self.args.host,
                    max_pages=self.args.max_pages,
                    sleep=self.args.sleep,
                    seen_ids=seen_ids,
                    since_id=self.args.since_id,
                    until_id=self.args.until_id,
                    early_coverage_seconds=self.args.early_coverage_seconds,
                )
                notes.extend(sub_notes)
        else:
            notes = self.client.fetch_notes(
                mode=self.args.mode,
                start=slot.start,
                end=slot.end,
                limit=self.args.limit,
                query=self.args.query,
                host=self.args.host,
                max_pages=self.args.max_pages,
                sleep=self.args.sleep,
                seen_ids=seen_ids,
                since_id=self.args.since_id,
                until_id=self.args.until_id,
                early_coverage_seconds=self.args.early_coverage_seconds,
            )
        if not self.args.keep_non_japanese:
            notes = filter_japanese_notes(notes)
        return notes

    def build_s3_key(self, slot: Slot) -> str:
        date_prefix = f"{slot.start.astimezone(JST):%Y/%m/%d/%H}"
        filename = f"{slot.timestamp}.jsonl"
        if self.dest_source.prefix:
            return f"{self.dest_source.prefix}/{date_prefix}/{filename}"
        return f"{date_prefix}/{filename}"

    def upload_notes(self, slot: Slot, notes: List[dict]) -> SlotReport:
        lines = []
        earliest: Optional[datetime] = None
        latest: Optional[datetime] = None

        for note in sorted(notes, key=lambda n: n.get("createdAt", "")):
            created_at = note.get("createdAt")
            if created_at:
                dt = parse_note_datetime(created_at).astimezone(JST)
                if earliest is None or dt < earliest:
                    earliest = dt
                if latest is None or dt > latest:
                    latest = dt
            lines.append(json.dumps(note, ensure_ascii=False))

        body = ("\n".join(lines) + "\n").encode("utf-8") if lines else b""
        key = self.build_s3_key(slot)
        metadata = {
            "slot": slot.timestamp,
            "note_count": str(len(notes)),
        }
        if earliest:
            metadata["earliest"] = earliest.isoformat()
        if latest:
            metadata["latest"] = latest.isoformat()

        self.s3_client.put_object(
            Bucket=self.dest_source.bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            Metadata=metadata,
        )
        coverage_ok = False
        if earliest:
            coverage_ok = earliest <= slot.start + timedelta(seconds=self.args.early_coverage_seconds)

        report = SlotReport(
            slot=slot,
            s3_key=key,
            note_count=len(notes),
            earliest=earliest,
            latest=latest,
            coverage_ok=coverage_ok,
        )
        self.logger.info(
            "Uploaded %s (%d notes, coverage_ok=%s)",
            slot.timestamp,
            report.note_count,
            report.coverage_ok,
        )
        return report

    def run(self) -> None:
        slots = self.build_slots()
        missing = self.detect_missing_slots(slots)
        self.logger.info(
            "Total slots: %d / Missing on S3 (%s): %d",
            len(slots),
            self.args.dataset,
            len(missing),
        )
        if not missing:
            self.logger.info("No missing slots detected. Nothing to do.")
            return
        if self.args.dry_run:
            for slot in missing:
                self.logger.info("DRY-RUN missing: %s", slot.timestamp)
            return

        missing_iter = self._iter_with_progress(missing, "Complementing missing slots")
        for idx, slot in enumerate(missing_iter, start=1):
            self.logger.info("[%d/%d] Complementing %s", idx, len(missing), slot.timestamp)
            notes = self.fetch_slot_notes(slot)
            report = self.upload_notes(slot, notes)
            self.slot_reports.append(report)

        self.logger.info("Uploaded %d complement files to s3://%s", len(self.slot_reports), self.dest_source.bucket)
        self.perform_verification(slots)

    def perform_verification(self, slots: Sequence[Slot]) -> None:
        """補完後に S3 側で欠損が残っていないか検証する。"""
        verify_inventory = S3SlotInventory(self.s3_client, self.sources + [self.dest_source])
        remaining = []
        verify_iter = self._iter_with_progress(list(slots), "Verifying slots")
        for slot in verify_iter:
            if not verify_inventory.slot_exists(slot.start, slot.timestamp):
                remaining.append(slot)

        if remaining:
            self.logger.warning("Verification failed: %d slots still missing.", len(remaining))
            for slot in remaining[:20]:
                self.logger.warning("  - %s", slot.timestamp)
        else:
            self.logger.info("Verification OK: no missing slots after including complement bucket.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AWS EC2 用 Misskey 欠損補完パイプライン")
    parser.add_argument("--start", required=True, type=parse_jst, help="開始日時 (JST, 例: 2025-08-01T00:00)")
    parser.add_argument("--end", required=True, type=parse_jst, help="終了日時 (JST, 例: 2025-08-02T23:50)")
    parser.add_argument("--slot-minutes", type=int, default=10, help="スロット幅（分）")
    parser.add_argument("--primary-bucket", default="miyazawa1s3", help="一次バケット名")
    parser.add_argument("--primary-prefix", default="misskey", help="一次バケット側のプレフィックス")
    parser.add_argument("--backup-bucket", default="miyazawa1s3-backup", help="バックアップバケット名")
    parser.add_argument("--backup-prefix", default="misskey", help="バックアップ側のプレフィックス")
    parser.add_argument("--dataset", choices=["jp", "en"], default="jp", help="欠損チェック対象 (jp: 一次, en: バックアップ)")
    parser.add_argument("--complement-bucket", default="miyazawa1s3", help="補完結果を保存するバケット")
    parser.add_argument("--complement-prefix", default="misskey_complement", help="補完結果のプレフィックス")
    parser.add_argument("--aws-region", help="AWS リージョン (例: ap-northeast-1)")
    parser.add_argument("--aws-profile", help="boto3 用の AWS プロファイル名")
    parser.add_argument("--token", help="Misskey API トークン (未指定時は MISSKEY_TOKEN 環境変数)")
    parser.add_argument("--base-url", default="https://misskey.io", help="Misskey ベース URL")
    parser.add_argument("--endpoint", default="notes/search", help="利用するエンドポイント")
    parser.add_argument("--mode", choices=["search", "timeline"], default="search", help="補完モード")
    parser.add_argument("--limit", type=int, default=100, help="Misskey API の limit")
    parser.add_argument("--max-pages", type=int, help="ページネーション上限")
    parser.add_argument("--sleep", type=float, default=5.0, help="API 呼び出し間隔（秒）")
    parser.add_argument("--sub-slot-seconds", type=int, default=60, help="補完時のサブスロット幅（秒）")
    parser.add_argument("--keep-non-japanese", action="store_true", help="日本語以外も保存する")
    parser.add_argument("--early-coverage-seconds", type=int, default=2, help="開始カバレッジ判定の閾値（秒）")
    parser.add_argument("--since-id", help="Misskey API sinceId")
    parser.add_argument("--until-id", help="Misskey API untilId")
    parser.add_argument("--query", help="notes/search の query")
    parser.add_argument("--host", help="notes/search の host フィルタ")
    parser.add_argument("--timeout", type=int, default=30, help="Misskey API タイムアウト秒")
    parser.add_argument("--retry", type=int, default=3, help="Misskey API リトライ回数")
    parser.add_argument("--retry-wait", type=float, default=5.0, help="Misskey API リトライ間隔（秒）")
    parser.add_argument("--dry-run", action="store_true", help="欠損状況の確認のみ行う")
    parser.add_argument("--progress", action="store_true", help="tqdm でプログレスバーを表示する")
    parser.add_argument("--max-slots", type=int, help="処理するスロット数を制限（デバッグ用）")
    parser.add_argument("--verbose", action="store_true", help="デバッグログを有効化")
    return parser


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.start_dt = args.start  # store parsed datetimes
    args.end_dt = args.end
    configure_logging(args.verbose)
    pipeline = AWSComplementPipeline(args)
    pipeline.run()


if __name__ == "__main__":
    main()
