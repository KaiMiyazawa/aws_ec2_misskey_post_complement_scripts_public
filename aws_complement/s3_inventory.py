"""S3 上の Misskey データを巡回して欠損スロットを把握するユーティリティ。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, Dict, Iterable, Optional, Set, Tuple, TYPE_CHECKING

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


class S3SlotInventory:
    """S3 から日単位でキー一覧を取得し、スロット存在判定を提供する。"""

    def __init__(
        self,
        s3_client: BaseClient,
        sources: Iterable[BucketSource],
    ) -> None:
        self._client = s3_client
        self._sources = list(sources)
        self._cache: Dict[str, Set[str]] = {}

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

    def _list_keys_for_day(self, source: BucketSource, slot_start: datetime) -> Set[str]:
        """指定日のキー一覧を取得し、ファイル名（拡張子なし）の集合を返す。"""
        local = slot_start.astimezone(JST)
        date_prefix = f"{local:%Y/%m/%d}/"
        if source.prefix:
            prefix = f"{source.prefix}/{date_prefix}"
        else:
            prefix = date_prefix
        paginator = self._client.get_paginator("list_objects_v2")
        key_set: Set[str] = set()

        for page in paginator.paginate(Bucket=source.bucket, Prefix=prefix):
            contents = page.get("Contents", [])
            for obj in contents:
                key = obj.get("Key")
                if not key or not key.endswith(".jsonl"):
                    continue
                filename = key.rsplit("/", 1)[-1]
                key_set.add(filename[:-6])  # remove ".jsonl"

        return key_set

    def _ensure_day_cached(self, day_key: str, slot_start: datetime) -> None:
        if day_key in self._cache:
            return
        combined: Set[str] = set()
        for source in self._sources:
            try:
                combined.update(self._list_keys_for_day(source, slot_start))
            except ClientError as exc:
                logging.warning(
                    "Failed to list objects for %s/%s on %s: %s",
                    source.bucket,
                    source.prefix,
                    day_key,
                    exc,
                )
                continue
        self._cache[day_key] = combined

    def slot_exists(self, slot_start: datetime, slot_ts: str) -> bool:
        """スロット（10分枠）のファイルがいずれかのソースに存在するか。"""
        day_key = self._day_key(slot_start)
        self._ensure_day_cached(day_key, slot_start)
        return slot_ts in self._cache.get(day_key, set())

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
