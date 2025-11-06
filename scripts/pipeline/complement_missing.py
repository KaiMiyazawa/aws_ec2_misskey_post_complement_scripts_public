#!/usr/bin/env python3
"""補完用データ収集スクリプト

Misskey API を利用して指定期間の欠損スロットを後追い取得し、
`data_complement/` 以下に JSON Lines 形式で保存する。

概要:
    - 10 分刻み（デフォルト）の時間枠を走査し、`data/` および
      `data_complement/` にファイルが存在しないスロットのみ取得。
    - API の呼び出しには Misskey の `/api/notes/search` または
      `notes/*timeline` エンドポイントを利用。
    - 取得件数が多い場合はページネーション（offset）で全件取得する。

使用例:
    python scripts/complement_missing.py \
        --start 2025-08-14T22:10 \
        --end 2025-08-15T01:30 \
        --token $MISSKEY_TOKEN

注意事項:
    - `sinceDate` / `untilDate` パラメータで範囲検索できるように Misskey 側で
      検索機能が有効化されている必要がある。
    - グローバルタイムラインを対象としているため、必要に応じて
      `--channel` や検索条件を調整して利用すること。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence

import requests

# 日本標準時（JST = UTC+9）のタイムゾーン定義
JST = timezone(timedelta(hours=9))

# デフォルト設定値
DEFAULT_BASE_URL = "https://misskey.io"  # Misskey インスタンスのベースURL
DEFAULT_ENDPOINT = "notes/search"  # 使用するAPIエンドポイント（検索用）
DEFAULT_LIMIT = 100  # API 1回あたりの取得件数上限
DEFAULT_SLOT_MINUTES = 10  # 1スロットの時間幅（分単位）
DEFAULT_MODE = "search"  # データ取得モード（search または timeline）
DEFAULT_CHECKPOINT_SLOTS = 1  # 何スロット分取得するごとに中間保存するか

def debug(msg: str) -> None:
    """デバッグメッセージを標準エラー出力に表示

    Args:
        msg: 出力するメッセージ
    """
    print(msg, file=sys.stderr)


def is_japanese(text: Optional[str]) -> bool:
    """テキストに日本語文字が含まれるかを判定

    平仮名・片仮名・漢字のいずれかが1文字でも含まれていればTrueを返す。
    既存の収集スクリプトと同じロジックを使用。

    Args:
        text: 判定対象のテキスト

    Returns:
        日本語文字が含まれる場合はTrue、それ以外はFalse
    """
    if not text:
        return False
    # Unicode範囲による簡易判定
    # 0x3040-0x30FF: 平仮名・片仮名
    # 0x4E00-0x9FFF: CJK統合漢字
    for ch in text:
        code = ord(ch)
        if (0x3040 <= code <= 0x30FF) or (0x4E00 <= code <= 0x9FFF):
            return True
    return False


@dataclass(frozen=True)
class Slot:
    """時間スロットを表すクラス（10分単位の時間枠）

    Attributes:
        start: スロット開始時刻（timezone-aware、JST）
        duration: スロットの時間幅
    """
    start: datetime  # timezone-aware (JST)
    duration: timedelta

    @property
    def end(self) -> datetime:
        """スロット終了時刻を返す

        Returns:
            開始時刻 + 時間幅 で計算された終了時刻
        """
        return self.start + self.duration

    @property
    def timestamp(self) -> str:
        """スロットのタイムスタンプ文字列を返す

        Returns:
            "YYYY-MM-DD_HH-MM" 形式の文字列（例: "2025-08-01_00-40"）
        """
        return self.start.strftime("%Y-%m-%d_%H-%M")

    @property
    def path_components(self) -> Sequence[str]:
        """ファイルパスの構成要素を返す

        Returns:
            [年, 月, 日, 時] の文字列タプル（例: ("2025", "08", "01", "00")）
        """
        return (
            self.start.strftime("%Y"),
            self.start.strftime("%m"),
            self.start.strftime("%d"),
            self.start.strftime("%H"),
        )

    def to_path(self, root: Path) -> Path:
        """ルートディレクトリからのファイルパスを生成

        Args:
            root: データのルートディレクトリ（data/ または data_complement/）

        Returns:
            完全なファイルパス（例: data/2025/08/01/00/2025-08-01_00-40.jsonl）
        """
        return root.joinpath(*self.path_components, f"{self.timestamp}.jsonl")


class MisskeyClient:
    """Misskey API クライアント

    `/api/notes/search` および `notes/*timeline` エンドポイントを使った
    ノート取得に対応。リトライ機能、レート制限対応を含む。

    Attributes:
        base_url: MisskeyインスタンスのベースURL（末尾のスラッシュは除去される）
        token: API認証トークン（必要に応じて設定）
        endpoint: 使用するAPIエンドポイント名
        session: HTTPリクエスト用のセッションオブジェクト
        timeout: API呼び出しのタイムアウト秒数
        retry: リトライ回数
        retry_wait: リトライ時の待機秒数
    """

    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        endpoint: str = DEFAULT_ENDPOINT,
        timeout: int = 30,
        retry: int = 3,
        retry_wait: float = 5.0,
    ):
        """MisskeyClientの初期化

        Args:
            base_url: MisskeyインスタンスのベースURL
            token: API認証トークン（オプション）
            endpoint: 使用するAPIエンドポイント名（デフォルト: "notes/search"）
            timeout: API呼び出しのタイムアウト秒数（デフォルト: 30）
            retry: リトライ回数（デフォルト: 3）
            retry_wait: リトライ時の待機秒数（デフォルト: 5.0）
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.endpoint = endpoint
        self.session = requests.Session()
        self.timeout = timeout
        self.retry = retry
        self.retry_wait = retry_wait

    def _post(self, endpoint: str, payload: dict) -> requests.Response:
        """Misskey APIにPOSTリクエストを送信（リトライ機能付き）

        429エラー（レート制限）の場合は自動的にリトライする。
        Retry-Afterヘッダーがあればその秒数、なければretry_wait秒待機する。

        Args:
            endpoint: APIエンドポイント名（"notes/search" など）
            payload: リクエストボディ（辞書形式）

        Returns:
            成功したHTTPレスポンス

        Raises:
            requests.HTTPError: リトライ上限に達した場合やその他のHTTPエラー
        """
        url = f"{self.base_url}/api/{endpoint}"
        body = dict(payload)
        # トークンがあればリクエストボディに "i" キーで追加
        if self.token:
            body.setdefault("i", self.token)

        attempt = 0
        last_exc: Optional[requests.HTTPError] = None
        while attempt <= self.retry:
            try:
                response = self.session.post(url, json=body, timeout=self.timeout)
                response.raise_for_status()
                return response
            except requests.HTTPError as exc:
                last_exc = exc
                status = exc.response.status_code if exc.response else None
                # 429エラー（レート制限）でかつリトライ上限に達していない場合のみリトライ
                if status != 429 or attempt == self.retry:
                    raise
                # Retry-Afterヘッダーから待機時間を取得
                wait_for = exc.response.headers.get("Retry-After") if exc.response else None
                try:
                    wait_seconds = float(wait_for) if wait_for else self.retry_wait
                except ValueError:
                    wait_seconds = self.retry_wait
                time.sleep(wait_seconds)
                attempt += 1

        assert last_exc is not None
        raise last_exc

    def fetch_notes_for_period(
        self,
        mode: str,
        period_start: datetime,
        period_end: datetime,
        limit: int = DEFAULT_LIMIT,
        query: Optional[str] = None,
        host: Optional[str] = None,
        max_pages: Optional[int] = None,
        sleep: float = 0.0,
        seen_ids: Optional[set[str]] = None,
        since_id: Optional[str] = None,
        until_id: Optional[str] = None,
        early_coverage_seconds: int = 30,
    ) -> Iterator[dict]:
        """Period全体をカバーするページングを行い、ノートを逐次yield

        searchモードではuntilIdベースのページネーションを使用。
        untilIdを段階的に更新（最も古いノートのIDを次のuntilIdに設定）し、
        期間開始から指定秒数以内のノートが取得できたら完了とする。

        Args:
            mode: 取得モード（"search" または "timeline"）
            period_start: Period開始時刻（timezone-aware）
            period_end: Period終了時刻（timezone-aware）
            limit: API 1回あたりの取得件数上限
            query: 検索クエリ（現在は使用されていない）
            host: 特定ホストに限定する場合に指定
            max_pages: ページネーションの上限回数
            sleep: API呼び出し間隔（秒）
            seen_ids: 既に取得済みのノートIDセット（重複除去用）
            since_id: 取得範囲の開始ID（このIDより後の投稿を取得）
            until_id: 取得範囲の終了ID（このIDより前の投稿を取得）
            early_coverage_seconds: 期間開始から何秒以内のノートが取得できたら完了とするか

        Yields:
            取得したノート（dict）を1件ずつ
        """

        # 重複除去用のIDセットを準備
        local_seen: set[str]
        if seen_ids is not None:
            local_seen = seen_ids
        else:
            local_seen = set()

        if mode == "search":
            # ========================================
            # searchモード: notes/search を使用
            # ========================================

            # 初期ペイロード設定
            payload = {
                "query": "",  # 空文字列で全ノート検索
                "limit": limit,
            }
            if since_id:
                payload["sinceId"] = since_id  # 開始ID（固定）
            if host:
                payload["host"] = host

            # untilIdを段階的に更新していく（重要な仕組み）
            current_until_id = until_id  # 最初のuntilIdは引数で渡されたもの
            early_threshold = period_start + timedelta(seconds=early_coverage_seconds)  # 開始から30秒後など
            zero_result_count = 0  # 連続して0件の回数をカウント
            page = 0  # ページ番号（デバッグ用）
            collected_oldest_dt = None  # 実際にyieldしたノートの最古時刻（カバレッジ判定用）

            debug(f"    Early coverage threshold: {early_threshold.strftime('%Y-%m-%d %H:%M:%S')}")
            debug(f"    Initial payload: sinceId={since_id}, untilId={current_until_id}")

            while True:
                # 現在のuntilIdをペイロードに設定
                if current_until_id:
                    payload["untilId"] = current_until_id
                elif "untilId" in payload:
                    # untilIdがNoneの場合は削除
                    del payload["untilId"]

                # API呼び出し
                resp = self._post(self.endpoint, payload)
                data = resp.json()
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected response: {data!r}")

                debug(f"    Page {page+1}: {len(data)} items returned (untilId={current_until_id})")

                # 終了判定1: 2回連続で0件が返された場合
                if len(data) == 0:
                    zero_result_count += 1
                    debug(f"    Zero results count: {zero_result_count}")
                    if zero_result_count >= 2:
                        debug(f"    Stopping: 2 consecutive zero results")
                        break
                else:
                    zero_result_count = 0

                # レスポンスの処理
                yielded = 0  # このページでyieldしたノート数
                oldest_note_id = None  # 最も古いノートのID（次のuntilIdに使用）
                oldest_note_dt = None  # 最も古いノートの時刻

                # 各ノートを処理
                for note in data:
                    created_at = note.get("createdAt")
                    note_id = note.get("id")
                    if not created_at or not note_id:
                        # createdAtまたはIDがないノートはスキップ
                        continue
                    dt = parse_note_datetime(created_at).astimezone(JST)

                    # 最も古いノートを追跡（次のuntilIdに使用するため）
                    if oldest_note_dt is None or dt < oldest_note_dt:
                        oldest_note_dt = dt
                        oldest_note_id = note_id

                    # 重複チェック
                    if note_id in local_seen:
                        continue
                    local_seen.add(note_id)

                    # ノートをyield
                    yield note
                    yielded += 1

                    # 実際にyieldしたノートの最古時刻を追跡（カバレッジ判定用）
                    if collected_oldest_dt is None or dt < collected_oldest_dt:
                        collected_oldest_dt = dt

                debug(f"    Yielded {yielded} notes from this page")

                # 終了判定2: 前半の指定秒数に到達（重要な終了条件）
                if collected_oldest_dt and collected_oldest_dt <= early_threshold:
                    debug(f"    Stopping: reached early coverage threshold ({collected_oldest_dt.strftime('%Y-%m-%d %H:%M:%S')} <= {early_threshold.strftime('%Y-%m-%d %H:%M:%S')})")
                    break

                # 終了判定3: max_pages制限に達した場合
                page += 1
                if max_pages is not None and page >= max_pages:
                    debug(f"    Stopping: reached max_pages={max_pages}")
                    break

                # untilIdを更新（最も古いノートのIDを次のuntilIdに）
                if oldest_note_id:
                    current_until_id = oldest_note_id
                    debug(f"    Updating untilId to: {current_until_id} (oldest note time: {oldest_note_dt.strftime('%Y-%m-%d %H:%M:%S')})")
                else:
                    debug(f"    No valid notes found, stopping")
                    break

                # API呼び出し間隔の待機
                if sleep:
                    time.sleep(sleep)

        else:
            # ========================================
            # timelineモード: notes/*timeline を使用
            # ========================================
            payload = {
                "limit": limit,
            }
            if since_id:
                payload["sinceId"] = since_id
            if until_id:
                payload["untilId"] = until_id
            if query:
                payload["query"] = query
            if host:
                payload["host"] = host

            page = 0
            next_until_id: Optional[str] = None
            prev_until_id: Optional[str] = None
            while True:
                if next_until_id:
                    payload["untilId"] = next_until_id
                elif "untilId" in payload:
                    payload.pop("untilId")

                resp = self._post(self.endpoint, payload)
                data = resp.json()
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected response: {data!r}")

                if not data:
                    break

                yielded = 0
                oldest_dt: Optional[datetime] = None
                oldest_id: Optional[str] = None
                for note in data:
                    created_at = note.get("createdAt")
                    note_id = note.get("id")
                    if not created_at or not note_id:
                        continue
                    dt = parse_note_datetime(created_at).astimezone(JST)

                    if oldest_dt is None or dt < oldest_dt:
                        oldest_dt = dt
                        oldest_id = note_id

                    if note_id in local_seen:
                        continue
                    local_seen.add(note_id)
                    yield note
                    yielded += 1

                page += 1
                if len(data) < limit:
                    break
                if max_pages is not None and page >= max_pages:
                    break
                if oldest_id is None or oldest_id == prev_until_id:
                    break
                prev_until_id = next_until_id
                next_until_id = oldest_id
                if sleep:
                    time.sleep(sleep)

    def fetch_notes(
        self,
        mode: str,
        start: datetime,
        end: datetime,
        limit: int = DEFAULT_LIMIT,
        query: Optional[str] = None,
        host: Optional[str] = None,
        max_pages: Optional[int] = None,
        sleep: float = 0.0,
        seen_ids: Optional[set[str]] = None,
        since_id: Optional[str] = None,
        until_id: Optional[str] = None,
        early_coverage_seconds: int = 30,
    ) -> List[dict]:
        """指定モードでノートを取得し、時間範囲でフィルタして返す

        searchモードではuntilIdベースのページネーションを使用。
        untilIdを段階的に更新（最も古いノートのIDを次のuntilIdに設定）し、
        期間開始から指定秒数以内のノートが取得できたら完了とする。

        Args:
            mode: 取得モード（"search" または "timeline"）
            start: 取得対象期間の開始時刻（timezone-aware）
            end: 取得対象期間の終了時刻（timezone-aware）
            limit: API 1回あたりの取得件数上限
            query: 検索クエリ（現在は使用されていない）
            host: 特定ホストに限定する場合に指定
            max_pages: ページネーションの上限回数
            sleep: API呼び出し間隔（秒）
            seen_ids: 既に取得済みのノートIDセット（重複除去用）
            since_id: 取得範囲の開始ID（このIDより後の投稿を取得）
            until_id: 取得範囲の終了ID（このIDより前の投稿を取得）
            early_coverage_seconds: 期間開始から何秒以内のノートが取得できたら完了とするか

        Returns:
            取得したノートのリスト（時間範囲でフィルタ済み、createdAtでソート済み）
        """

        # sinceDate/untilDateは機能しないためコメントアウト
        # since_ms = int(start.astimezone(timezone.utc).timestamp() * 1000)
        # until_ms = int(end.astimezone(timezone.utc).timestamp() * 1000)

        # 重複除去用のIDセットを準備
        local_seen: set[str]
        if seen_ids is not None:
            local_seen = seen_ids
        else:
            local_seen = set()
        collected: List[dict] = []

        if mode == "search":
            # ========================================
            # searchモード: notes/search を使用
            # ========================================

            # 初期ペイロード設定
            payload = {
                "query": "",  # 空文字列で全ノート検索
                "limit": limit,
            }
            if since_id:
                payload["sinceId"] = since_id  # 開始ID（固定）
            if host:
                payload["host"] = host

            # untilIdを段階的に更新していく（重要な仕組み）
            current_until_id = until_id  # 最初のuntilIdは引数で渡されたもの
            early_threshold = start + timedelta(seconds=early_coverage_seconds)  # 開始から30秒後など
            zero_result_count = 0  # 連続して0件の回数をカウント
            page = 0  # ページ番号（デバッグ用）

            debug(f"    Early coverage threshold: {early_threshold.strftime('%Y-%m-%d %H:%M:%S')}")
            debug(f"    Initial payload: sinceId={since_id}, untilId={current_until_id}")

            while True:
                # 現在のuntilIdをペイロードに設定
                if current_until_id:
                    payload["untilId"] = current_until_id
                elif "untilId" in payload:
                    # untilIdがNoneの場合は削除
                    del payload["untilId"]

                # API呼び出し
                resp = self._post(self.endpoint, payload)
                data = resp.json()
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected response: {data!r}")

                debug(f"    Page {page+1}: {len(data)} items returned (untilId={current_until_id})")

                # 終了判定1: 2回連続で0件が返された場合
                # （これ以上データがないと判断）
                if len(data) == 0:
                    zero_result_count += 1
                    debug(f"    Zero results count: {zero_result_count}")
                    if zero_result_count >= 2:
                        debug(f"    Stopping: 2 consecutive zero results")
                        break
                else:
                    zero_result_count = 0

                # レスポンスの処理
                added = 0  # このページで追加したノート数
                filtered_out_time = 0  # 時間範囲外でフィルタされたノート数
                min_dt_in_response = None  # このページの最古時刻
                max_dt_in_response = None  # このページの最新時刻
                oldest_note_id = None  # 最も古いノートのID（次のuntilIdに使用）
                oldest_note_dt = None  # 最も古いノートの時刻
                collected_oldest_dt = None  # 実際に保存されたノートの最古時刻（カバレッジ判定用）

                # 各ノートを処理
                for note in data:
                    created_at = note.get("createdAt")
                    note_id = note.get("id")
                    if not created_at or not note_id:
                        # createdAtまたはIDがないノートはスキップ
                        continue
                    dt = parse_note_datetime(created_at).astimezone(JST)

                    # レスポンスの時刻範囲を記録（デバッグ用）
                    if min_dt_in_response is None or dt < min_dt_in_response:
                        min_dt_in_response = dt
                    if max_dt_in_response is None or dt > max_dt_in_response:
                        max_dt_in_response = dt

                    # 最も古いノートを追跡（次のuntilIdに使用するため）
                    # Misskey IDは時系列順なので、最も古いIDを保存する
                    if oldest_note_dt is None or dt < oldest_note_dt:
                        oldest_note_dt = dt
                        oldest_note_id = note_id

                    # 時刻範囲でフィルタ（start <= dt < end）
                    if not (start <= dt < end):
                        filtered_out_time += 1
                        continue
                    # 重複チェック
                    if note_id in local_seen:
                        continue
                    local_seen.add(note_id)
                    collected.append(note)
                    added += 1

                    # 実際に保存されたノートの最古時刻を追跡（カバレッジ判定用）
                    if collected_oldest_dt is None or dt < collected_oldest_dt:
                        collected_oldest_dt = dt

                # デバッグ出力: 期待する時刻範囲と実際の時刻範囲
                debug(f"    Expected: {start.strftime('%Y-%m-%d %H:%M:%S')} ~ {end.strftime('%Y-%m-%d %H:%M:%S')}")
                if min_dt_in_response and max_dt_in_response:
                    debug(f"    Actual:   {min_dt_in_response.strftime('%Y-%m-%d %H:%M:%S')} ~ {max_dt_in_response.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    debug(f"    Actual:   No valid timestamps in response")
                if filtered_out_time > 0:
                    debug(f"    Filtered out {filtered_out_time} notes (time range mismatch)")
                debug(f"    Added {added} notes to collection")

                # 終了判定2: 前半の指定秒数に到達（重要な終了条件）
                # 期間開始から30秒以内（デフォルト）のノートが実際に保存できたら、
                # それ以上遡る必要はないと判断して終了
                # 注: フィルタで除外されたノートではなく、実際に保存されたノートで判定
                if collected_oldest_dt and collected_oldest_dt <= early_threshold:
                    debug(f"    Stopping: reached early coverage threshold ({collected_oldest_dt.strftime('%Y-%m-%d %H:%M:%S')} <= {early_threshold.strftime('%Y-%m-%d %H:%M:%S')})")
                    break

                # 終了判定3: max_pages制限に達した場合
                page += 1
                if max_pages is not None and page >= max_pages:
                    debug(f"    Stopping: reached max_pages={max_pages}")
                    break

                # untilIdを更新（最も古いノートのIDを次のuntilIdに）
                # これにより、次のAPI呼び出しでさらに過去のノートを取得できる
                if oldest_note_id:
                    current_until_id = oldest_note_id
                    debug(f"    Updating untilId to: {current_until_id} (oldest note time: {oldest_note_dt.strftime('%Y-%m-%d %H:%M:%S')})")
                else:
                    debug(f"    No valid notes found, stopping")
                    break

                # API呼び出し間隔の待機
                if sleep:
                    time.sleep(sleep)

        else:
            # ========================================
            # timelineモード: notes/*timeline を使用
            # ========================================
            # 注: このモードは現在あまり使用されていない
            payload = {
                "limit": limit,
                # sinceDate/untilDateはMisskey側で正常に動作しないためコメントアウト
                # "sinceDate": since_ms,
                # "untilDate": until_ms,
            }
            # sinceId/untilIdを使用してID範囲指定
            if since_id:
                payload["sinceId"] = since_id
            if until_id:
                payload["untilId"] = until_id
            if query:
                payload["query"] = query
            if host:
                payload["host"] = host

            page = 0
            next_until_id: Optional[str] = None  # 次のページのuntilId
            prev_until_id: Optional[str] = None  # 前回のuntilId（ループ検出用）
            while True:
                if next_until_id:
                    payload["untilId"] = next_until_id
                elif "untilId" in payload:
                    payload.pop("untilId")

                resp = self._post(self.endpoint, payload)
                data = resp.json()
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected response: {data!r}")

                if not data:
                    break

                added = 0
                oldest_dt: Optional[datetime] = None
                oldest_id: Optional[str] = None
                for note in data:
                    created_at = note.get("createdAt")
                    note_id = note.get("id")
                    if not created_at or not note_id:
                        continue
                    dt = parse_note_datetime(created_at).astimezone(JST)
                    # `end` より新しいノートもページングの基準として利用する
                    if dt >= end:
                        if oldest_dt is None or dt < oldest_dt:
                            oldest_dt = dt
                            oldest_id = note_id
                        continue
                    if dt < start:
                        # さらに古いものは必要ないが、ページング継続のために基準を更新
                        if oldest_dt is None or dt < oldest_dt:
                            oldest_dt = dt
                            oldest_id = note_id
                        continue
                    if note_id in local_seen:
                        continue
                    local_seen.add(note_id)
                    collected.append(note)
                    added += 1
                    if oldest_dt is None or dt < oldest_dt:
                        oldest_dt = dt
                        oldest_id = note_id

                page += 1
                if len(data) < limit:
                    break
                if max_pages is not None and page >= max_pages:
                    break
                if oldest_id is None or oldest_id == prev_until_id:
                    break
                prev_until_id = next_until_id
                next_until_id = oldest_id
                if sleep:
                    time.sleep(sleep)

        collected.sort(
            key=lambda note: parse_note_datetime(note["createdAt"]) if note.get("createdAt") else start
        )
        return collected


def parse_note_datetime(value: str) -> datetime:
    """MisskeyのcreatedAt文字列をdatetimeオブジェクトに変換

    MisskeyのcreatedAtはISO8601形式（例: "2025-08-01T00:40:10.764Z"）。
    timezone-aware（UTC）のdatetimeオブジェクトとして返す。

    Args:
        value: ISO8601形式の日時文字列

    Returns:
        timezone-aware（UTC）のdatetimeオブジェクト
    """
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def iter_slots(start: datetime, end: datetime, minutes: int) -> Iterator[Slot]:
    """指定期間をスロット単位で分割して列挙

    Args:
        start: 開始日時
        end: 終了日時
        minutes: スロットの時間幅（分）

    Yields:
        Slotオブジェクト（開始時刻と時間幅を持つ）
    """
    step = timedelta(minutes=minutes)
    current = start
    while current <= end:
        yield Slot(start=current, duration=step)
        current += step


def iter_sub_ranges(start: datetime, end: datetime, delta: timedelta) -> Iterator[tuple[datetime, datetime]]:
    """期間をさらに細かく分割して列挙（サブスロット機能）

    Args:
        start: 開始日時
        end: 終了日時
        delta: 分割する時間幅

    Yields:
        (sub_start, sub_end) のタプル

    Raises:
        ValueError: deltaが0以下の場合
    """
    if delta.total_seconds() <= 0:
        raise ValueError("sub-slot delta must be positive")
    current = start
    while current < end:
        sub_end = current + delta
        if sub_end > end:
            sub_end = end
        yield current, sub_end
        current = sub_end


def load_existing_slots(paths: Iterable[Path]) -> set[str]:
    """既存のデータファイルからスロットのタイムスタンプセットを取得

    data/ と data_complement/ の両方から *.jsonl ファイルを走査し、
    ファイル名（拡張子なし）をタイムスタンプとして収集する。

    Args:
        paths: 走査するルートディレクトリのリスト

    Returns:
        タイムスタンプ文字列のセット（例: {"2025-08-01_00-40", ...}）
    """
    timestamps = set()
    for base in paths:
        if not base.exists():
            continue
        for file in base.rglob("*.jsonl"):
            try:
                timestamps.add(file.stem)
            except Exception:
                continue
    return timestamps


def ensure_parent(path: Path) -> None:
    """ファイルの親ディレクトリを作成（存在しない場合）

    Args:
        path: ファイルパス
    """
    path.parent.mkdir(parents=True, exist_ok=True)


def save_notes(path: Path, notes: Sequence[dict]) -> None:
    """ノートをJSON Lines形式でファイルに保存

    Args:
        path: 保存先ファイルパス
        notes: 保存するノートのリスト
    """
    ensure_parent(path)
    with path.open("w", encoding="utf-8") as f:
        for note in notes:
            json.dump(note, f, ensure_ascii=False)
            f.write("\n")


def filter_japanese_notes(notes: Iterable[dict]) -> List[dict]:
    """日本語を含むノートのみをフィルタリング

    Args:
        notes: ノートのリスト

    Returns:
        日本語を含むノートのみのリスト
    """
    filtered: List[dict] = []
    for note in notes:
        text = note.get("text")
        if is_japanese(text):
            filtered.append(note)
    return filtered


def classify_note_to_slot(note_dt: datetime) -> str:
    """ノートの createdAt を10分スロットに分類

    Args:
        note_dt: ノートの作成日時（JST）

    Returns:
        スロットタイムスタンプ（"YYYY-MM-DD_HH-MM"形式）

    Example:
        >>> dt = datetime(2025, 8, 18, 8, 47, 32, tzinfo=JST)
        >>> classify_note_to_slot(dt)
        "2025-08-18_08-40"
    """
    minute = (note_dt.minute // 10) * 10
    slot_dt = note_dt.replace(minute=minute, second=0, microsecond=0)
    return slot_dt.strftime("%Y-%m-%d_%H-%M")


def is_slot_covered(slot: Slot, oldest_note_dt: datetime, early_coverage_seconds: int) -> bool:
    """スロットが完了したか判定

    Args:
        slot: 判定対象のスロット
        oldest_note_dt: 現在取得済みの最古ノート時刻
        early_coverage_seconds: 早期完了判定の閾値（秒）

    Returns:
        True: スロットが完了した
        False: まだ完了していない
    """
    early_threshold = slot.start + timedelta(seconds=early_coverage_seconds)
    return oldest_note_dt <= early_threshold


def save_slot_file(slot: Slot, notes: List[dict], complement_root: Path) -> None:
    """スロット単位でファイルを保存

    Args:
        slot: 保存対象のスロット
        notes: 保存するノートのリスト
        complement_root: 補完データのルートディレクトリ
    """
    # createdAt でソート（古い順）
    notes.sort(key=lambda n: parse_note_datetime(n["createdAt"]) if n.get("createdAt") else slot.start)

    # ファイルパスを生成
    target_path = slot.to_path(complement_root)
    save_notes(target_path, notes)
    debug(f"Saved {len(notes)} notes -> {target_path}")


def save_remaining_buffers(
    slot_buffers: dict[str, List[dict]],
    all_slots_dict: dict[str, Slot],
    complement_root: Path,
    keep_non_japanese: bool
) -> None:
    """中断時に残りのバッファを保存

    Args:
        slot_buffers: スロットバッファ（{スロットタイムスタンプ: [ノート]}）
        all_slots_dict: 全スロットの辞書（{タイムスタンプ: Slot}）
        complement_root: 補完データのルートディレクトリ
        keep_non_japanese: 日本語フィルタを無効化するか
    """
    for slot_ts, notes in slot_buffers.items():
        if not notes:
            continue

        notes_to_save = notes
        if not keep_non_japanese:
            notes_to_save = filter_japanese_notes(notes)

        if notes_to_save:
            slot = all_slots_dict[slot_ts]
            save_slot_file(slot, notes_to_save, complement_root)
            print(f"Saved interrupted slot {slot_ts}: {len(notes_to_save)} notes", file=sys.stderr)


def save_accumulated_notes(
    notes: List[dict],
    complement_root: Path,
    keep_non_japanese: bool = True
) -> Tuple[int, int]:
    """蓄積されたノートを10分スロットに分類して保存

    複数スロット分のノートをまとめて処理し、createdAtに基づいて
    10分スロットごとに分類してファイル保存する。
    中間保存機能で使用される。

    Args:
        notes: 蓄積されたノートのリスト
        complement_root: 補完データのルートディレクトリ
        keep_non_japanese: 日本語フィルタを無効化するか（True=無効化、False=適用）

    Returns:
        (保存したスロット数, 保存したノート数) のタプル
    """
    if not notes:
        return 0, 0

    # 日本語フィルタ（keep_non_japanese が False の場合のみ適用）
    if not keep_non_japanese:
        notes = filter_japanese_notes(notes)

    # ノートをcreatedAtに基づいて10分スロットごとに分類
    notes_by_slot: dict[str, List[dict]] = {}
    for note in notes:
        created_at = note.get("createdAt")
        if not created_at:
            continue
        note_dt = parse_note_datetime(created_at).astimezone(JST)

        # 10分スロットに丸める（例: 00:43 -> 00:40、00:47 -> 00:40）
        minute = (note_dt.minute // 10) * 10
        slot_dt = note_dt.replace(minute=minute, second=0, microsecond=0)

        # スロットのタイムスタンプを生成（YYYY-MM-DD_HH-MM形式）
        slot_timestamp = slot_dt.strftime("%Y-%m-%d_%H-%M")

        if slot_timestamp not in notes_by_slot:
            notes_by_slot[slot_timestamp] = []
        notes_by_slot[slot_timestamp].append(note)

    # 各スロットごとにファイルを保存
    saved_slots = 0
    saved_notes = 0
    for slot_timestamp, slot_notes in notes_by_slot.items():
        # タイムスタンプからSlotオブジェクトを作成
        slot_dt = datetime.strptime(slot_timestamp, "%Y-%m-%d_%H-%M").replace(tzinfo=JST)
        slot_obj = Slot(start=slot_dt, duration=timedelta(minutes=10))

        # ノートをcreatedAtでソート（古い順）
        slot_notes.sort(
            key=lambda note: parse_note_datetime(note["createdAt"]) if note.get("createdAt") else slot_dt
        )

        # complement_root 以下に保存
        target_path = slot_obj.to_path(complement_root)
        save_notes(target_path, slot_notes)
        saved_slots += 1
        saved_notes += len(slot_notes)
        debug(f"Saved {len(slot_notes)} notes -> {target_path}")

    return saved_slots, saved_notes


def main_period_mode(
    args: argparse.Namespace,
    start_dt: datetime,
    end_dt: datetime,
    client: MisskeyClient,
    all_slots: List[Slot],
    complement_root: Path,
) -> int:
    """Period単位でノートを取得し、ストリーミング保存する

    Args:
        args: コマンドライン引数
        start_dt: Period開始時刻
        end_dt: Period終了時刻
        client: MisskeyClientインスタンス
        all_slots: 全スロットのリスト
        complement_root: 補完データのルートディレクトリ

    Returns:
        終了コード（0=成功、1=エラー）
    """
    # スロット定義の生成
    all_slots_dict = {slot.timestamp: slot for slot in all_slots}
    slot_buffers: dict[str, List[dict]] = {}
    completed_slots: List[str] = []
    saved_slots = 0
    total_notes = 0

    print(f"対象スロット数: {len(all_slots)}")
    print(f"Period: {start_dt.strftime('%Y-%m-%d %H:%M')} ～ {end_dt.strftime('%Y-%m-%d %H:%M')}")
    print(f"Mode: period-based streaming pagination")

    # ページングの実行（ジェネレータ）
    try:
        debug(f"Starting period-based fetch...")
        note_generator = client.fetch_notes_for_period(
            mode=args.mode,
            period_start=start_dt,
            period_end=end_dt,
            limit=args.limit,
            query=args.query,
            host=args.host,
            max_pages=args.max_pages,
            sleep=args.sleep,
            seen_ids=set(),
            since_id=args.since_id,
            until_id=args.until_id,
            early_coverage_seconds=args.early_coverage_seconds,
        )

        oldest_note_dt = end_dt  # 現在取得中の最古ノート時刻
        note_count = 0  # 取得したノート総数（デバッグ用）

        for note in note_generator:
            note_count += 1

            # ノートをスロットに分類
            note_dt = parse_note_datetime(note["createdAt"]).astimezone(JST)
            oldest_note_dt = min(oldest_note_dt, note_dt)

            slot_ts = classify_note_to_slot(note_dt)
            if slot_ts not in all_slots_dict:
                # period範囲外のノートはスキップ
                continue

            # バッファに追加
            if slot_ts not in slot_buffers:
                slot_buffers[slot_ts] = []
            slot_buffers[slot_ts].append(note)

            # スロット完了判定
            slot = all_slots_dict[slot_ts]
            if is_slot_covered(slot, oldest_note_dt, args.early_coverage_seconds):
                if slot_ts not in completed_slots:
                    completed_slots.append(slot_ts)
                    debug(f"✓ Slot {slot_ts} covered (oldest={oldest_note_dt.strftime('%Y-%m-%d %H:%M:%S')}, buffer={len(slot_buffers[slot_ts])} notes)")

            # バッファ戦略: 1スロット遅れて保存
            if len(completed_slots) >= 2:
                slot_to_save = completed_slots[-2]

                # 既に保存済みの場合はスキップ（バッファから削除済み）
                if slot_to_save in slot_buffers:
                    notes_to_save = slot_buffers[slot_to_save]

                    if not args.keep_non_japanese:
                        notes_to_save = filter_japanese_notes(notes_to_save)

                    if notes_to_save:
                        save_slot_file(all_slots_dict[slot_to_save], notes_to_save, complement_root)
                        saved_slots += 1
                        total_notes += len(notes_to_save)
                        print(f"[{saved_slots}/{len(all_slots)}] Saved slot {slot_to_save}: {len(notes_to_save)} notes")

                    # メモリ解放
                    del slot_buffers[slot_to_save]

        debug(f"Pagination completed. Total notes fetched: {note_count}")

        # 残りのバッファを保存
        debug(f"Saving remaining {len(slot_buffers)} slots...")
        remaining_completed = [ts for ts in completed_slots if ts in slot_buffers]

        # completed_slotsに入っていないが、バッファにあるスロットも保存
        all_remaining = sorted(slot_buffers.keys())

        for slot_ts in all_remaining:
            notes_to_save = slot_buffers[slot_ts]
            if not args.keep_non_japanese:
                notes_to_save = filter_japanese_notes(notes_to_save)

            if notes_to_save:
                save_slot_file(all_slots_dict[slot_ts], notes_to_save, complement_root)
                saved_slots += 1
                total_notes += len(notes_to_save)
                status = "completed" if slot_ts in completed_slots else "partial"
                print(f"[{saved_slots}/{len(all_slots)}] Saved remaining slot {slot_ts} ({status}): {len(notes_to_save)} notes")

    except requests.HTTPError as exc:
        # エラーハンドリング
        if exc.response and exc.response.status_code == 429:
            print("\n[ERROR] レートリミットに到達しました", file=sys.stderr)
            # 未保存のバッファを保存
            save_remaining_buffers(slot_buffers, all_slots_dict, complement_root, args.keep_non_japanese)
            return 1
        # その他のHTTPエラー
        print(f"\n[ERROR] HTTP error: {exc}", file=sys.stderr)
        if exc.response:
            print(f"Response: {exc.response.text[:500]}", file=sys.stderr)
        # バッファを保存してから終了
        save_remaining_buffers(slot_buffers, all_slots_dict, complement_root, args.keep_non_japanese)
        return 1
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] 中断されました", file=sys.stderr)
        save_remaining_buffers(slot_buffers, all_slots_dict, complement_root, args.keep_non_japanese)
        return 130
    except Exception as exc:
        print(f"\n[ERROR] Unexpected error: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        save_remaining_buffers(slot_buffers, all_slots_dict, complement_root, args.keep_non_japanese)
        return 1

    print(f"\n補完完了: 保存スロット {saved_slots} / 対象 {len(all_slots)}、総ノート数 {total_notes}")
    return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Misskey欠損データ補完ツール")
    parser.add_argument("--start", required=True, help="開始日時 (例: 2025-08-15T00:00)")
    parser.add_argument("--end", required=True, help="終了日時 (例: 2025-08-16T23:50)")
    parser.add_argument(
        "--slot-minutes",
        type=int,
        default=DEFAULT_SLOT_MINUTES,
        help="スロット幅（分）",
    )
    parser.add_argument(
        "--sub-slot-seconds",
        type=int,
        help="スロットをさらに分割する秒数。指定すると各スロットをこの秒数単位で取得し結合する",
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Misskey API ベースURL (例: https://misskey.io)",
    )
    parser.add_argument("--token", help="Misskey API トークン (環境変数 MISSKEY_TOKEN でも指定可)")
    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
        help="利用する Misskey API エンドポイント (デフォルト: notes/search)",
    )
    parser.add_argument(
        "--mode",
        choices=["search", "timeline"],
        default=DEFAULT_MODE,
        help="取得方法を指定。search は notes/search、timeline は timeline 系エンドポイントを利用",
    )
    parser.add_argument(
        "--query",
        help="検索クエリ（非推奨：現在は使用されていません）",
    )
    parser.add_argument(
        "--host",
        help="特定ホストに限定する場合に指定 (notes/search の host パラメータ)",
    )
    parser.add_argument(
        "--since-id",
        help="取得範囲の開始ID（この IDより後の投稿を取得）",
    )
    parser.add_argument(
        "--until-id",
        help="取得範囲の終了ID（このIDより前の投稿を取得）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="notes/search 1回あたりの取得件数",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        help="ページネーションの上限。制限したい場合に指定",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="ページネーション時のAPI呼び出し間隔(秒)",
    )
    parser.add_argument(
        "--data-root",
        default="data",
        help="既存データのルートディレクトリ",
    )
    parser.add_argument(
        "--complement-root",
        default="data_complement",
        help="補完データを保存するルートディレクトリ",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="取得・保存を行わず欠損スロットのみ表示",
    )
    parser.add_argument(
        "--keep-non-japanese",
        action="store_true",
        help="日本語以外のノートも保存する (デフォルトは除外)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="補完先に既存ファイルがあっても上書きする",
    )
    parser.add_argument(
        "--early-coverage-seconds",
        type=int,
        default=2,
        help="期間開始から何秒以内のノートが取得できたら完了とするか (デフォルト: 2秒)",
    )
    parser.add_argument(
        "--checkpoint-slots",
        type=int,
        default=DEFAULT_CHECKPOINT_SLOTS,
        help=f"何スロット分取得するごとに中間保存するか (デフォルト: {DEFAULT_CHECKPOINT_SLOTS}, legacy-modeのみ)",
    )
    parser.add_argument(
        "--period-mode",
        action="store_true",
        default=True,
        help="Period単位でページングする（推奨、デフォルト）",
    )
    parser.add_argument(
        "--legacy-mode",
        action="store_true",
        help="従来のスロット単位処理を使用（非推奨）",
    )
    return parser.parse_args(argv)


def parse_jst_datetime(value: str) -> datetime:
    """日時文字列をJSTのdatetimeオブジェクトに変換

    Args:
        value: "YYYY-MM-DDTHH:MM" 形式の日時文字列

    Returns:
        JST timezone-aware のdatetimeオブジェクト

    Raises:
        argparse.ArgumentTypeError: 形式が不正な場合
    """
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M")
        return dt.replace(tzinfo=JST)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid datetime format: {value}") from exc


def main(argv: Optional[Sequence[str]] = None) -> int:
    """メイン処理

    Args:
        argv: コマンドライン引数（テスト用、通常はNone）

    Returns:
        終了コード（0=成功、1=エラー）
    """
    # コマンドライン引数をパース
    args = parse_args(argv)

    # 開始・終了日時をパース
    try:
        start_dt = parse_jst_datetime(args.start)
        end_dt = parse_jst_datetime(args.end)
    except argparse.ArgumentTypeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    if end_dt < start_dt:
        print("[ERROR] end must be greater than or equal to start", file=sys.stderr)
        return 1

    # API設定
    mode = args.mode
    endpoint = args.endpoint
    token = args.token or os.environ.get("MISSKEY_TOKEN")
    if mode == "search":
        if not token:
            print("[ERROR] notes/search を利用するには API トークンの指定が推奨されます。", file=sys.stderr)
            return 1

    # データディレクトリ設定
    data_root = Path(args.data_root)
    complement_root = Path(args.complement_root)
    sub_slot_seconds = args.sub_slot_seconds
    if sub_slot_seconds is not None and sub_slot_seconds <= 0:
        print("[ERROR] --sub-slot-seconds には正の整数を指定してください。", file=sys.stderr)
        return 1

    # 既存のスロット（data/ と data_complement/ の両方）を読み込む
    existing = load_existing_slots([data_root, complement_root])

    # 全スロットを列挙し、既存でないもののみを取得対象とする
    all_slots = list(iter_slots(start_dt, end_dt, args.slot_minutes))
    slots_to_fetch: List[Slot] = []
    for slot in all_slots:
        timestamp = slot.timestamp
        # 既存スロットはスキップ（--overwrite が指定されている場合は除く）
        if timestamp in existing and not args.overwrite:
            continue
        slots_to_fetch.append(slot)

    print(f"対象スロット数: {len(slots_to_fetch)} / 全スロット {len(all_slots)}")
    if args.dry_run:
        # dry-run モード: 取得対象のスロットを表示するだけで終了
        for slot in slots_to_fetch:
            print(slot.timestamp)
        return 0

    # Misskey APIクライアントを初期化
    client = MisskeyClient(base_url=args.base_url, token=token, endpoint=endpoint)

    # モード選択: --legacy-mode が指定されている場合は従来の処理
    if args.legacy_mode:
        print("Mode: legacy (slot-by-slot)")
        return main_legacy_mode(args, client, slots_to_fetch, complement_root)
    else:
        # 新しいperiod-modeを使用
        return main_period_mode(args, start_dt, end_dt, client, slots_to_fetch, complement_root)


def main_legacy_mode(
    args: argparse.Namespace,
    client: MisskeyClient,
    slots_to_fetch: List[Slot],
    complement_root: Path,
) -> int:
    """従来のスロット単位の処理（互換性のため残されている）

    Args:
        args: コマンドライン引数
        client: MisskeyClientインスタンス
        slots_to_fetch: 取得対象スロットのリスト
        complement_root: 補完データのルートディレクトリ

    Returns:
        終了コード（0=成功、1=エラー）
    """
    saved_slots = 0  # 保存したスロット数
    total_notes = 0  # 保存した総ノート数

    # 中間保存用のバッファとカウンタ
    accumulated_notes: List[dict] = []  # 複数スロット分のノートを蓄積
    checkpoint_counter = 0  # チェックポイントカウンタ

    # sub_slot_seconds の取得
    sub_slot_seconds = args.sub_slot_seconds

    # 各スロットについてノートを取得
    for idx, slot in enumerate(slots_to_fetch, 1):
        debug(f"[{idx}/{len(slots_to_fetch)}] Fetching {slot.timestamp} ...")
        slot_seen_ids: set[str] = set()  # このスロット内での重複除去用
        slot_notes: List[dict] = []  # このスロットで取得したノート

        # サブスロット分割が指定されている場合は分割
        if sub_slot_seconds is not None and sub_slot_seconds < slot.duration.total_seconds():
            sub_delta = timedelta(seconds=sub_slot_seconds)
            sub_ranges = list(iter_sub_ranges(slot.start, slot.end, sub_delta))
        else:
            # 分割なしの場合はスロット全体を1つの範囲として扱う
            sub_ranges = [(slot.start, slot.end)]

        debug(f"[{idx}/{len(slots_to_fetch)}] Fetching {slot.timestamp} ({len(sub_ranges)} sub-slot(s)) ...")

        # 各サブスロットについてノートを取得
        for sub_idx, (sub_start, sub_end) in enumerate(sub_ranges, 1):
            try:
                # APIを使ってノートを取得
                sub_notes = client.fetch_notes(
                    mode=args.mode,
                    start=sub_start,
                    end=sub_end,
                    limit=args.limit,
                    query=args.query,
                    host=args.host,
                    max_pages=args.max_pages,
                    sleep=args.sleep,
                    seen_ids=slot_seen_ids,
                    since_id=args.since_id,
                    until_id=args.until_id,
                    early_coverage_seconds=args.early_coverage_seconds,
                )
                # 取得件数を含めて表示
                debug(
                    f"  - sub-slot {sub_idx}/{len(sub_ranges)}: {sub_start.strftime('%H:%M:%S')} - {sub_end.strftime('%H:%M:%S')} → {len(sub_notes)} notes"
                )
            except requests.HTTPError as exc:
                # HTTPエラーが発生した場合の処理
                debug(f"  - sub-slot {sub_idx}/{len(sub_ranges)}: {sub_start.strftime('%H:%M:%S')} - {sub_end.strftime('%H:%M:%S')} → HTTP error: {exc}")
                if exc.response is not None:
                    debug(f"    response body: {exc.response.text[:500]}")
                    # レートリミットエラー（429）の場合はプログラムを停止
                    if exc.response.status_code == 429:
                        print("\n[ERROR] レートリミットに到達しました。", file=sys.stderr)
                        # 蓄積済みデータを保存してから終了
                        if accumulated_notes:
                            debug("Saving accumulated notes before exit...")
                            slots, notes = save_accumulated_notes(accumulated_notes, complement_root, args.keep_non_japanese)
                            print(f"中間保存: {slots} スロット、{notes} ノート", file=sys.stderr)
                        print("しばらく時間を置いてから、--resume オプションで再開してください。", file=sys.stderr)
                        return 1  # エラー終了
                # その他のHTTPエラーはスキップして次へ
                continue
            except Exception as exc:
                # その他のエラーもスキップ
                debug(f"  - sub-slot {sub_idx}/{len(sub_ranges)}: {sub_start.strftime('%H:%M:%S')} - {sub_end.strftime('%H:%M:%S')} → Error: {exc}")
                continue

            slot_notes.extend(sub_notes)
            # サブスロット間の待機（最後のサブスロットでは待機しない）
            if args.sleep and sub_idx < len(sub_ranges):
                time.sleep(args.sleep)

        if not slot_notes:
            debug(f"No notes collected for {slot.timestamp}")
            continue

        # スロットで取得したノートをバッファに追加
        accumulated_notes.extend(slot_notes)
        checkpoint_counter += 1

        # チェックポイント到達時に中間保存
        if checkpoint_counter >= args.checkpoint_slots:
            debug(f"Checkpoint reached ({checkpoint_counter} slots), saving accumulated notes...")
            slots, notes = save_accumulated_notes(accumulated_notes, complement_root, args.keep_non_japanese)
            saved_slots += slots
            total_notes += notes
            print(f"中間保存: {slots} スロット、{notes} ノート")

            # バッファとカウンタをリセット
            accumulated_notes = []
            checkpoint_counter = 0

        # スロット間の待機
        time.sleep(args.sleep)

    # ループ終了後、残りの蓄積データを保存
    if accumulated_notes:
        debug("Saving remaining accumulated notes...")
        slots, notes = save_accumulated_notes(accumulated_notes, complement_root, args.keep_non_japanese)
        saved_slots += slots
        total_notes += notes
        print(f"最終保存: {slots} スロット、{notes} ノート")

    print(f"補完完了: 保存スロット {saved_slots} / リクエスト {len(slots_to_fetch)}、総ノート数 {total_notes}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
