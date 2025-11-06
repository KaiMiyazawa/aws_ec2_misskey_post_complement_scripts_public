# 補完スクリプトの使い方

`scripts/pipeline/complement_missing.py` は、既存の `data/` ディレクトリで欠損している 10 分刻みのスロットを Misskey API から後追い取得し、`data_complement/` 以下に保存します。収集済みデータと同じファイル名・ディレクトリ構造で出力されるため、そのまま分析に利用できます。`notes/search` を利用する「search」モードと、`notes/global-timeline` などで `sinceDate` / `untilDate` に対応している場合に使える「timeline」モードを切り替えられます。さらに `--sub-slot-seconds` を指定すると、1 スロットを細かく分割して順次取得・重複除去したうえで結合できます。

## 前提条件

- Misskey の API トークン（環境変数 `MISSKEY_TOKEN` または `--token` で指定）
- Misskey サーバー側で `/api/notes/search` による期間検索が利用可能であること
- 欠損期間（開始・終了時刻）が JST で分かっていること

## 基本的な実行例

```bash
python scripts/pipeline/complement_missing.py \
  --start 2025-08-14T22:10 \
  --end 2025-08-23T08:50 \
  --mode timeline \
  --query '*' \
  --token "$MISSKEY_TOKEN"
```

### 推奨コマンド例（1 分刻み補完）

欠損を確実に埋めるため、`--sub-slot-seconds` で 1 分刻みに分割して補完するのが基本です。

```bash
python scripts/pipeline/complement_missing.py \
  --start 2025-07-16T12:10 \
  --end 2025-07-16T12:10 \
  --mode timeline \
  --endpoint notes/global-timeline \
  --limit 100 \
  --sub-slot-seconds 60 \
  --sleep 5 \
  --overwrite \
  --token "$MISSKEY_TOKEN"
```

より細かく 30 秒刻みにしたい場合は `--sub-slot-seconds 30` に変更します。

### notes/search を利用した補完

notes/search モード専用のラッパー `scripts/pipeline/complement_missing_search.py` も用意しています。

```bash
python scripts/pipeline/complement_missing_search.py \
  --start 2025-07-16T12:10 \
  --end 2025-07-16T12:20 \
  --token "$MISSKEY_TOKEN"
```

- `--sub-slot-seconds` を付けると時間範囲を細分化して取得します。
- 既定で `--query '*' --limit 100 --max-pages 100 --sleep 5 --overwrite` を指定した状態で `complement_missing.py` を呼び出します。

> **補足:** Timeline API (`notes/global-timeline`) だけでは、時間範囲の前半が返ってこないケースがあります。10 分枠の投稿数が多い日時や長期欠損を埋める際は、まず timeline で大まかに補完し、足りない部分をさらにこの notes/search モードで補うと取りこぼしを減らせます。

### 補完＋チェックをまとめて実行する

最小限の入力で補完とチェックを行うラッパー `scripts/pipeline/run_complement_and_verify.py` が利用できます。

- `--token` で API トークンを指定するか、環境変数 `MISSKEY_TOKEN` を設定してください。

```bash
python scripts/pipeline/run_complement_and_verify.py 2025-07-16_12-10 \
  --token "$MISSKEY_TOKEN"
```

- `--use-search` を付けると notes/search モードで補完します。
- `--sub-slot-seconds` で 30 秒刻みなどに変更可能です。

- `--start`, `--end` は JST で指定し、どちらも 10 分刻みの値を推奨します。
- スクリプトは対象期間を 10 分刻みで走査し、`data/` および `data_complement/` に同名ファイルが存在しないスロットのみ取得します。
- 取得したファイルは `data_complement/YYYY/MM/DD/HH/YYYY-MM-DD_HH-MM.jsonl` として保存されます。

## 主なオプション

- `notes/search` を利用する際は `--query` を必ず指定してください。全件対象にする場合は `--query '*'` のように指定します。
- Misskey のバージョンによっては `notes/global-timeline` などで `sinceDate` / `untilDate` を利用できるため、その場合は `--mode timeline` と `--endpoint notes/global-timeline` を併用すると時間範囲で取得できます。本スクリプトでは `id` ベースで重複を排除しつつページングします。

- `--slot-minutes`: スロット幅（既定値 10）
- `--base-url`: Misskey API のベース URL（既定値 `https://misskey.io`）
- `--mode`: `search` または `timeline` を指定。`timeline` の場合は `notes/global-timeline` 等を利用
- `--endpoint`: 利用するエンドポイント（既定値 `notes/search`）
- `--query`: `search` モードで利用するクエリ文字列（例: `*`, `lang:ja`）
- `--sub-slot-seconds`: 1 スロットをこの秒数単位に分割して取得し結合（例: `--sub-slot-seconds 60` で 1 分刻み）
- `--limit`: 1 リクエストあたりの取得件数（既定値 100）
- `--max-pages`: ページネーションの上限
- `--sleep`: ページネーション時やスロット間のウェイト秒数（レート制限対策）
- `--dry-run`: 実際の取得を行わず、取得対象となるスロットだけ表示
- `--keep-non-japanese`: 日本語判定をスキップして全ノートを保存
- `--overwrite`: `data_complement/` に同名ファイルがあっても上書き

## 欠損スロットの確認

`--dry-run` を使うと API 呼び出しを行わずに欠損スロットの一覧を確認できます。取得前に対象の広がりを把握したい場合に便利です。

```bash
python scripts/pipeline/complement_missing.py \
  --start 2025-08-14T22:10 \
  --end 2025-08-23T08:50 \
  --mode timeline \
  --query '*' \
  --dry-run
```

```bash
python scripts/pipeline/complement_missing.py \
  --start 2025-07-16T12:10 \
  --end 2025-07-16T12:10 \
  --mode timeline \
  --endpoint notes/global-timeline \
  --limit 100 \
  --sub-slot-seconds 30 \
  --sleep 5 \
  --overwrite \
  --token "$MISSKEY_TOKEN"
```

## 取得ログと結果

- 標準エラー出力に進捗ログを表示し、各スロットの取得件数を報告します。
- 処理完了後、保存できたスロット件数と総ノート数のサマリを標準出力に表示します。

### 補完後のチェック

補完が完了したら `scripts/check_slot_coverage.py` で結果を確認できます。

```bash
python scripts/check_slot_coverage.py 2025-07-16_12-10
```

- 統合行数やユニーク ID、`createdAt` の最小・最大が表示され、10 分枠をカバーできているかを確認できます。
- `--show-empty` を付けると、ファイルが存在しない場合も出力します。

## 注意事項

- `notes/search` が利用できない Misskey 環境では、別のエンドポイントを `--endpoint` で指定し、パラメータ構成を適宜調整してください（例: `notes/global-timeline`）。
- 取得データは元のスクリプト同様、日本語文字を含む投稿のみに限定されます。全量を保存したい場合は `--keep-non-japanese` を指定してください。
- `data_complement/` に保存されたファイルは既存データとは別ディレクトリにあり、分析時に統合する際は二つのルートを合わせて参照する必要があります。
