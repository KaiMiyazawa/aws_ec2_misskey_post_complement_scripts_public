# AWS Complement Pipeline

AWS EC2 上で Misskey の欠損データを検出・補完し、新しい S3 バケットに保存して検証するための運用手順をまとめました。既存のローカル向けスクリプトと同じ Misskey API ロジックを内部で再利用しつつ、S3 を直接参照する構成になっています。

---

## 目的と背景

REF_jri_misskey_posts_collector では `data_upload.py`（東京リージョン）と `data_upload_en.py`（バージニアリージョン）がそれぞれ `miyazawa1s3/misskey` と `miyazawa1s3-backup/misskey` に10分刻みの JSONL を転送しています。本ドキュメントで紹介する `aws_complement/run_pipeline.py` は、これら2つのバケットを **`--dataset jp` / `--dataset en` の切り替えで選択的に走査** して欠損を特定し、Misskey API から補完したデータを **第3の領域（例: `miyazawa1s3/misskey_complement`）** に保存します。保存後は再度 S3 側を確認し、欠損が解消しているかを検証します。

EC2 インスタンスはメモリ／ディスクが潤沢ではない前提で、以下の点に気を配っています。

- S3 は日単位でリスト化し、結果をメモリキャッシュすることで `head_object` の大量発行を避ける。
- Misskey から取得したノートはスロット単位に逐次アップロードし、ローカルには残さない。
- 補完結果は S3 メタデータに統計情報を付与し、後続の確認を容易にする。

---

## 必要環境

1. Python 3.10 以上
2. `pip install -r requirements.txt` に加え、EC2 上で `boto3`, `requests`, `matplotlib`, `tqdm` が利用できること
3. Misskey API トークン（`MISSKEY_TOKEN` 環境変数、または `--token` 引数で指定）
4. AWS 認証情報（`aws configure` もしくは環境変数/Instance Profile）
5. 参照・補完用の S3 バケット
    - 既存: `miyazawa1s3/misskey`（JP 用）, `miyazawa1s3-backup/misskey`（EN 用）
    - 新設: `miyazawa1s3/misskey_complement`（名称は `--complement-bucket/prefix` で変更可能）

---

## ディレクトリ構成

```
aws_complement/
├── __init__.py
├── run_pipeline.py        # エントリポイント
└── s3_inventory.py        # S3 から日次キャッシュを構築するユーティリティ
README_AWS_COMPLEMENT.md   # 本ドキュメント
```

---

## 使い方

### 1. 欠損検出 → 補完 → 検証を一括実行

```bash
python aws_complement/run_pipeline.py \
  --start 2025-08-01T00:00 \
  --end 2025-08-10T23:50 \
  --token "$MISSKEY_TOKEN" \
  --aws-region ap-northeast-1 \
  --dataset jp \
  --primary-bucket miyazawa1s3 \
  --primary-prefix misskey \
  --backup-bucket miyazawa1s3-backup \
  --backup-prefix misskey \
  --complement-bucket miyazawa1s3 \
  --complement-prefix misskey_complement \
  --mode search \
  --sub-slot-seconds 60 \
  --sleep 5 \
  --progress
```

#### 主要引数

| 引数 | 説明 | 既定値 |
|------|------|--------|
| `--start` / `--end` | JST の開始/終了スロット。`2025-08-01T00:00` 形式で入力 | 必須 |
| `--slot-minutes` | スロット幅（分） | 10 |
| `--dataset` | 欠損判定対象 (`jp` で一次, `en` でバックアップ) | `jp` |
| `--primary-*` / `--backup-*` | `jp`/`en` 選択時に使う S3 バケット名 | `miyazawa1s3` / `miyazawa1s3-backup` |
| `--complement-*` | 補完結果を置くバケット/プレフィックス | `miyazawa1s3` / `misskey_complement` |
| `--token` | Misskey API トークン | `MISSKEY_TOKEN` |
| `--mode` | `search` か `timeline` | `search` |
| `--sub-slot-seconds` | 1 スロットを細分化して取得する秒数 | 60 |
| `--early-coverage-seconds` | カバレッジ判定用の閾値 | 30 |
| `--sleep` | Misskey API のページング間隔 | 5 |
| `--progress` | `tqdm` プログレスバーを表示 | 無効 |
| `--dry-run` | 欠損状況を表示するだけで補完しない | 無効 |

### 2. 欠損状況のみ確認（JP または EN を選択）

```
python aws_complement/run_pipeline.py \
  --start 2025-08-01T00:00 \
  --end 2025-08-05T23:50 \
  --dry-run --verbose
```

`--dry-run` では S3 をスキャンして欠損スロットをログ出力するだけです。EC2 の小規模インスタンスで様子を見たいときに利用してください。進捗が見たい場合は `--progress` を付けると `tqdm` バーが表示されます（`pip install tqdm` 済みであること）。

バックアップ（EN）側だけを確認する場合は `--dataset en --backup-bucket miyazawa1s3-backup` のように指定します。`--dataset` の値に応じて、対応するバケット/プレフィックスが参照されます。

---

## 内部フロー

1. **S3 スキャン**  
   `aws_complement/s3_inventory.py` が日単位で `list_objects_v2` を呼び出し、`YYYY-MM-DD` → `スロット名` のキャッシュを構築します。これにより、欠損判定はメモリ上のセット照会のみで完結し、EC2 からの API 呼び出しを最小限に抑えます。

2. **欠損スロットの抽出**  
   `scripts/pipeline/complement_missing.py` に定義済みの `Slot`/`iter_slots` を再利用し、指定期間の 10 分枠を列挙。S3 に `.jsonl` が存在しても **行数が 100 以下または 10,000 以上（エラーデータ）であれば欠損扱い** としてリストアップします。

3. **Misskey API で補完**  
   既存の `MisskeyClient` をモジュールとしてロードし、`notes/search` (または timeline 系 API) を呼び出します。`--sub-slot-seconds` に応じて 1 分刻みなどで細分化し、`seen_ids` を共有しながら重複を排除します。取得したノートはその場で JSON Lines にシリアライズし、ローカルファイルを作らずに S3 へアップロードします。

4. **S3 へ保存**  
   デフォルトでは `s3://miyazawa1s3/misskey_complement/YYYY/MM/DD/HH/スロット.jsonl` に保存。S3 メタデータにノート数・最古/最新時刻を記録しておくことで、後工程での Spot チェックが容易になります。

5. **検証**  
   補完後は一次・バックアップ・補完バケットのすべてを対象に再度 S3 キャッシュを構築し、欠損が残っていないかを確認します。未補完スロットがあれば最大 20 件までログに出力し、レート制限や API エラーが起きた箇所を追跡できます。

---

### 欠損判定ルール（デフォルト）

1. `.jsonl` が存在しない → 欠損
2. 行数が 100 行以下 → 欠損（データ欠落）
3. 行数が 10,000 行以上 → 欠損（異常値として再取得）
4. 101〜9,999 行 → 正常

行数チェックは実ファイルをストリーム読み込みして行うため、大量期間を一括スキャンすると時間がかかります。必要に応じて期間を絞るか、`--max-slots` で分割実行してください。

---

## 運用上のヒント

1. **インスタンスサイズを抑える**  
   スクリプトは 1 スロットずつ順次処理し、完了後にメモリを解放します。t3.small などでも運用可能ですが、バックオフ（`--sleep`）を 5〜10 秒程度にして Misskey への負荷を抑えてください。

2. **S3 バケットのライフサイクル**  
   補完バケットは長期保管用（例: `GLACIER_IR`）にライフサイクルルールを設定しておくとコストを抑えられます。補完結果をメインのミラーへ統合済みなら、90 日後に削除するポリシーを付与するのも有効です。

3. **二重化された入力の利用**  
   `miyazawa1s3` と `miyazawa1s3-backup` のどちらかにファイルが存在すれば欠損扱いから除外されます。JP 側のアップローダーが一時停止しても、US 側のフェイルセーフデータを自動で参照します。

4. **段階的な補完**  
   `--max-slots` を使うと、最初の N スロットだけ様子を見てから本番実行に移れます。大規模欠損（数千スロット）の際は 200〜300 スロット単位で分割運用するのが安全です。

5. **ログ/監視**  
   `--verbose` で DEBUG ログを有効化すると、S3 の日次キャッシュや Misskey ページネーションの詳細が出力されます。CloudWatch Agent で `/var/log/aws_complement.log` などに転送すると便利です。

---

## 失敗時のリカバリ

| 症状 | 対処 |
|------|------|
| Misskey API が 429 を返す | `--sleep` と `--retry-wait` を増やして再実行。未補完スロットのみ処理されます。 |
| 補完後も欠損が残る | ログ末尾に残件が表示されるので、そのスロットだけ `--max-slots` と `--start/--end` を狭めて再実行。 |
| S3 への書き込み権限がない | EC2 IAM ロールに `s3:PutObject` (補完バケット) と `s3:ListBucket` (全バケット) を付与。 |
| バケットを切り替えたい | `--primary-*` や `--complement-*` を引数で指定するだけで運用先を変えられます。 |

---

## 今後の拡張アイデア

1. **Step Functions への移植**  
   欠損検出→補完→検証を AWS Step Functions でオーケストレーションすれば、Long-Running Process を避けながら冪等に再実行できます。

2. **Athena/Glue と連携**  
   補完バケットを Glue テーブル化し、Athena クエリでカバレッジを集計すれば、補完後のギャップを SQL ベースで検証できます。

3. **Lambda トリガー**  
   `miyazawa1s3` で一定時間ファイルが増えなかった場合に SNS で通知し、自動で補完ジョブを起動する仕組みも検討できます。

---

以上の構成をベースに、EC2 上での欠損補完フローを自動化してください。不明点があれば `aws_complement/run_pipeline.py --help` を参照のうえ、必要なオプションを追加してください。レビューや改善提案も歓迎です。 
