"""S3 上の Misskey データを巡回して欠損スロットを把握するユーティリティ。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple, TYPE_CHECKING

try:  # pragma: no cover - boto3 runtime dependency
    from botocore.exceptions import ClientError
except ImportError:  # pragma: no cover
    class ClientError(Exception):  # type: ignore
        """Placeholder when botocore is unavailable (e.g., --help)."""
        pass

if TYPE_CHECKING:
    from botocore.client import BaseClient  # pragma: no cover
else:
    BaseClient = Any

JST = timezone(timedelta(hours=9))


@dataclass(frozen=True)
class BucketSource:
    """欠損判定対象となる S3 バケットとプレフィックスの組。"""

    bucket: str
    prefix: str


@dataclass
class SlotObjectRef:
    """S3 上の特定スロットファイルへの参照と検証結果。"""

    bucket: str
    key: str
    line_count: Optional[int] = None
    valid: Optional[bool] = None


MIN_VALID_LINES = 100
MAX_VALID_LINES = 9999


class S3SlotInventory:
    """S3 から日単位でキー一覧を取得し、スロット存在判定を提供する。"""

    def __init__(
        self,
        s3_client: BaseClient,
        sources: Iterable[BucketSource],
    ) -> None:
        self._client = s3_client
        self._sources = list(sources)
        self._cache: Dict[str, Dict[str, List[SlotObjectRef]]] = {}

    @staticmethod
    def _day_key(slot_start: datetime) -> str:
        return slot_start.astimezone(JST).strftime("%Y-%m-%d")

    @staticmethod
    def slot_key_from_timestamp(slot_ts: str) -> Tuple[str, str, str, str]:
        """スロットタイムスタンプから (YYYY, MM, DD, HH) を返す。"""
        dt = datetime.strptime(slot_ts, "%Y-%m-%d_%H-%M").replace(tzinfo=JST)
        return (
            dt.strftime("%Y"),
            dt.strftime("%m"),
            dt.strftime("%d"),
            dt.strftime("%H"),
        )

    def _list_keys_for_day(self, source: BucketSource, slot_start: datetime) -> Dict[str, List[SlotObjectRef]]:
        """指定日のキー一覧を取得し、スロットごとの参照リストを返す。"""
        local = slot_start.astimezone(JST)
        date_prefix = f"{local:%Y/%m/%d}/"
        if source.prefix:
            prefix = f"{source.prefix}/{date_prefix}"
        else:
            prefix = date_prefix
        paginator = self._client.get_paginator("list_objects_v2")
        slot_map: Dict[str, List[SlotObjectRef]] = {}

        for page in paginator.paginate(Bucket=source.bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            for obj in contents:
                key = obj.get("Key")
                if not key or not key.endswith(".jsonl"):
                    continue
                filename = key.rsplit("/", 1)[-1]
                slot_ts = filename[:-6]
                slot_map.setdefault(slot_ts, []).append(SlotObjectRef(bucket=source.bucket, key=key))

        return slot_map

    def _ensure_day_cached(self, day_key: str, slot_start: datetime) -> None:
        if day_key in self._cache:
            return
        combined: Dict[str, List[SlotObjectRef]] = {}
        for source in self._sources:
            try:
                day_refs = self._list_keys_for_day(source, slot_start)
            except ClientError as exc:
                logging.warning(
                    "Failed to list objects for %s/%s on %s: %s",
                    source.bucket,
                    source.prefix,
                    day_key,
                    exc,
                )
                continue
            for slot_ts, refs in day_refs.items():
                combined.setdefault(slot_ts, []).extend(refs)
        self._cache[day_key] = combined

    def _is_valid_object(self, ref: SlotObjectRef) -> bool:
        """S3 オブジェクトの行数を検証し、欠損扱いかどうかを返す。"""
        try:
            response = self._client.get_object(Bucket=ref.bucket, Key=ref.key)
        except ClientError as exc:
            logging.warning("Failed to fetch %s/%s: %s", ref.bucket, ref.key, exc)
            ref.line_count = None
            return False

        body = response["Body"]
        line_count = 0
        try:
            for line in body.iter_lines():
                line_count += 1
                if line_count >= MAX_VALID_LINES + 1:
                    ref.line_count = line_count
                    return False
        finally:
            body.close()

        ref.line_count = line_count
        if line_count <= MIN_VALID_LINES:
            return False
        return True

    def slot_exists(self, slot_start: datetime, slot_ts: str) -> bool:
        """スロット（10分枠）のファイルがいずれかのソースに存在するか。"""
        day_key = self._day_key(slot_start)
        self._ensure_day_cached(day_key, slot_start)
        day_slots = self._cache.get(day_key, {})
        refs = day_slots.get(slot_ts)
        if not refs:
            return False

        for ref in refs:
            if ref.valid is None:
                ref.valid = self._is_valid_object(ref)
            if ref.valid:
                return True
        return False

    def refresh_cache(self) -> None:
        """EC2 長期稼働時に呼び出してキャッシュをクリアする。"""
        self._cache.clear()


def build_s3_client(region_name: Optional[str] = None, profile: Optional[str] = None) -> BaseClient:
    """boto3 の S3 クライアントを構築する。"""
    try:
        import boto3  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("boto3 が必要です。`pip install boto3` を実行してください。") from exc

    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    session = boto3.session.Session(**session_kwargs)
    return session.client("s3", region_name=region_name)
