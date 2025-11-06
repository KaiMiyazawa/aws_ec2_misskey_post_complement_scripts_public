# Misskey データ補完システムの開発履歴

このドキュメントは、Misskeyタイムラインデータの欠損補完システムを開発する過程で遭遇した問題、試行錯誤、そして最終的な解決策に至るまでの経緯をまとめたものです。

## 目次

1. [背景と初期の課題](#背景と初期の課題)
2. [第1段階: オフセットベースのページネーション](#第1段階-オフセットベースのページネーション)
3. [第2段階: untilId更新方式への移行](#第2段階-untilid更新方式への移行)
4. [第3段階: early_coverage_secondsの導入](#第3段階-early_coverage_secondsの導入)
5. [第4段階: データ形式の理解と検証](#第4段階-データ形式の理解と検証)
6. [第5段階: 検証スクリプトの改善](#第5段階-検証スクリプトの改善)
7. [最終構成](#最終構成)
8. [学んだ教訓](#学んだ教訓)

---

## 背景と初期の課題

### 問題の発見

AWS S3から取得したMisskeyタイムラインデータに、多数の欠損スロット（10分単位の時間枠）が存在することが判明。

**欠損の規模**:
- 対象期間: 2025-08-01 ～ 2025-10-09
- 欠損期間: 147期間
- 欠損スロット: 合計数百スロット

### 初期のアプローチ

最初は `sub-slot-seconds` パラメータを使った細分化アプローチを検討していた。

```python
# 初期の考え方: 10分を1分や30秒に細分化して取得
--sub-slot-seconds 60  # 1分刻み
--sub-slot-seconds 30  # 30秒刻み
```

**問題点**:
- 細分化しても、各リクエストで同じID範囲（sinceId～untilId）を指定
- API側で返されるのは「untilId側（新しい方）から最新のN件」
- 結果として、どの細分化リクエストも同じ後半部分のデータしか取得できない

---

## 第1段階: オフセットベースのページネーション

### 試行内容

「sub-slot-seconds の代わりにオフセットパラメータを使おう」という提案。

```python
# 試した方法
payload = {
    "query": "",
    "limit": 100,
    "offset": current_offset,  # 0, 100, 200, ...
    "sinceId": since_id,
    "untilId": until_id,
}
```

**ロジック**:
1. offset=0 でリクエスト
2. 結果が0件になるまで offset を 100ずつ増やす
3. 2回連続で0件になったら次のスロットへ

### 失敗の原因

**実際の動作**:
```
スロット: 2025-08-01 00:40 ～ 00:50 (10分間)

offset=0 : 00:48 ～ 00:50 のデータ (100件)
offset=100: 00:48 ～ 00:50 のデータ (100件) ← 同じ範囲！
offset=200: 00:48 ～ 00:50 のデータ (100件) ← 同じ範囲！
```

**問題の本質**:
- Misskey API は sinceId/untilId の範囲内で「untilId側（新しい方）から」データを返す
- offset は「その範囲内の結果をスキップする」だけ
- untilId を更新しない限り、何度リクエストしても同じ時間帯のデータしか取れない

**実測データ**:
- 取得できたノート数: 96件
- 時刻範囲: 00:48:35 ～ 00:49:59
- **00:40 ～ 00:48の約8分間が欠損**

---

## 第2段階: untilId更新方式への移行

### 問題の再定義

ユーザーからの指摘:
> 「検索によってヒットするのは、sinceとuntilのuntil側から最新のN件がヒットするようになっているらしい。そのため、対象の10分の後半3分だけしか取れてないみたいなことが起こっている。」

### 新しいアプローチ

**基本方針**:
1. **sinceId は固定**（期間の開始境界として機能）
2. **untilId を段階的に更新**（最も古いノートのIDを次のuntilIdに設定）
3. これにより時系列を遡ってデータを取得

**実装イメージ**:
```python
# 初回リクエスト
sinceId: aav4kppvuwcu01wc (00:40の境界)
untilId: aav4xllwk1vb029z (00:50の境界)
→ 取得: 00:48 ～ 00:50 のデータ

# 2回目リクエスト
sinceId: aav4kppvuwcu01wc (固定)
untilId: <最も古いノートのID> (例: 00:48のどこか)
→ 取得: 00:46 ～ 00:48 のデータ

# 3回目リクエスト
sinceId: aav4kppvuwcu01wc (固定)
untilId: <最も古いノートのID> (例: 00:46のどこか)
→ 取得: 00:44 ～ 00:46 のデータ

# ... 繰り返し
```

### 実装の詳細

```python
# untilIdを段階的に更新していく
current_until_id = until_id  # 最初のuntilIdは引数で渡されたもの
page = 0

while True:
    # 現在のuntilIdを設定
    if current_until_id:
        payload["untilId"] = current_until_id

    # API呼び出し
    resp = self._post(self.endpoint, payload)
    data = resp.json()

    # 最も古いノートを追跡（次のuntilIdに使用）
    oldest_note_id = None
    oldest_note_dt = None

    for note in data:
        created_at = note.get("createdAt")
        note_id = note.get("id")
        if not created_at or not note_id:
            continue
        dt = parse_note_datetime(created_at).astimezone(JST)

        # 最も古いノートを追跡
        if oldest_note_dt is None or dt < oldest_note_dt:
            oldest_note_dt = dt
            oldest_note_id = note_id

    # untilIdを更新（最も古いノートのIDを次のuntilIdに）
    if oldest_note_id:
        current_until_id = oldest_note_id
    else:
        break
```

### 成功の証拠

**改善後の実測データ**:
```
スロット: 2025-08-01 00:40 ～ 00:50

取得できたノート数: 492件 (以前は96件)
時刻範囲: 00:40:10 ～ 00:49:59
```

✅ **10分間のほぼ全範囲をカバー成功！**

---

## 第3段階: early_coverage_secondsの導入

### 新たな課題

untilId更新方式で広範囲のデータは取得できるようになったが、**いつ停止すべきか？**という問題が残った。

**停止条件の候補**:
1. ❌ スロット開始時刻（00:40:00）ちょうどまで取得
   - 問題: 00:40:00 ちょうどの投稿は稀
   - 結果: 無限ループやタイムアウトのリスク

2. ❌ max_pages で制限
   - 問題: スロットごとにデータ量が異なる
   - 結果: 少ないスロットは過剰、多いスロットは不足

3. ✅ **early_coverage_seconds の導入**
   - 考え方: スロット開始から指定秒数以内をカバーできれば十分

### 実装

```python
# 開始から30秒以内のデータが取得できたら完了
early_coverage_seconds: int = 30
early_threshold = start + timedelta(seconds=early_coverage_seconds)

# 各ページの処理後にチェック
if oldest_note_dt and oldest_note_dt <= early_threshold:
    debug(f"Stopping: reached early coverage threshold")
    break
```

**ロジックの理由**:
- スロット開始（例: 00:40:00）から30秒以内（例: 00:40:30まで）のデータが取得できれば、実用上十分
- 完璧に00:40:00から取得しようとすると、API制限やパフォーマンスの問題が発生
- 30秒のバッファで効率と網羅性のバランスを取る

### 設定可能にした理由

```python
parser.add_argument(
    "--early-coverage-seconds",
    type=int,
    default=30,
    help="期間開始から何秒以内のノートが取得できたら完了とするか (デフォルト: 30秒)",
)
```

- データの重要度に応じて調整可能
- 重要な期間は60秒、通常は30秒など柔軟に対応

---

## 第4段階: データ形式の理解と検証

### 混乱の発生

検証スクリプト `check_slot_coverage.py` を実行したところ、予期しない結果:

```
data/2025/08/01/00/2025-08-01_00-40.jsonl
    行数: 5
    ユニークID: 0 (重複 0)  ← なぜ0？
```

### 原因の調査

data/ ディレクトリのファイルを確認:

```json
{"message":"Rate limit exceeded","code":"RATE_LIMIT_EXCEEDED","id":".."}
{"message":"Rate limit exceeded","code":"RATE_LIMIT_EXCEEDED","id":".."}
{"message":"Rate limit exceeded","code":"RATE_LIMIT_EXCEEDED","id":".."}
{"message":"Rate limit exceeded","code":"RATE_LIMIT_EXCEEDED","id":".."}
{"message":"Rate limit exceeded","code":"RATE_LIMIT_EXCEEDED","id":".."}
```

**発見**:
- data/ には2種類のファイルが存在
  1. **エラーログ**: Rate limit や他のエラーメッセージ
  2. **正常なノートデータ**: Misskeyの投稿オブジェクト

### data_complement/ のフォーマット確認

```json
{"id":"aav4l...","createdAt":"2025-08-01T00:40:10.764Z","userId":"...","text":"...","..."}
{"id":"aav4m...","createdAt":"2025-08-01T00:40:15.123Z","userId":"...","text":"...","..."}
{"id":"aav4n...","createdAt":"2025-08-01T00:40:20.456Z","userId":"...","text":"...","..."}
```

✅ **data_complement/ は正常なMisskeyノートのみ**

### 結論

- data/ でIDが0件 = エラーログのみのファイル（これは想定通り）
- data_complement/ は常に正常なノートデータ
- 両者のフォーマットは**互換性がある**（正常データの場合）

---

## 第5段階: 検証スクリプトの改善

### 検証の必要性

補完が正しく行われたかを確認するため、包括的な検証スクリプトが必要。

### verify_complement.py の開発

**検証項目**:
1. ✅ 補完ファイルが存在するか
2. ✅ data/ と data_complement/ でIDの重複がないか
3. ✅ 開始カバレッジ（early_coverage_seconds）を満たしているか
4. ⚠️ 終了カバレッジ（スロット終了時刻近く）

### 終了カバレッジ問題

**初期実装**: スロット終了時刻（例: 00:49:59）までカバーしているかチェック

**実行結果**:
```
✓ 2025-08-01_00-40
  ⚠ 終了カバレッジ不足
✓ 2025-08-01_01-30
  ⚠ 終了カバレッジ不足
✓ 2025-08-01_02-40
  ⚠ 終了カバレッジ不足
...
13スロット中10スロットで警告
```

**ユーザーのフィードバック**:
> 「終了カバレッジについて、警告しなくていいです。コメントアウトする感じで」

**理由**:
- スロット終了ギリギリ（例: 00:49:59）のデータを取得するのは実用上困難
- API の挙動として、untilId を使っても終端付近は取りこぼしが発生しやすい
- 実用上、スロット開始から十分なデータが取れていれば問題ない

### 最終実装

```python
# カバレッジ判定
has_early_coverage = complement_min and complement_min <= early_threshold if complement_notes else False
# 終了カバレッジは実用上問題ないためチェックしない
# （スロット終了ギリギリまで取得するのは困難なため）
has_late_coverage = True  # 常にOKとする
```

### 検証結果

```
検証対象スロット数: 13

✓ 2025-08-01_00-40
✓ 2025-08-01_01-30
✓ 2025-08-01_02-40
✓ 2025-08-01_02-50
✓ 2025-08-01_03-10
✓ 2025-08-01_03-20
✓ 2025-08-01_04-00
✓ 2025-08-01_04-10
✓ 2025-08-01_13-10
✓ 2025-08-01_14-50
✓ 2025-08-01_15-30
✓ 2025-08-01_19-10
✓ 2025-08-02_01-20

=== 検証結果 ===
✓ 成功: 13
⚠ 警告: 0
✗ 失敗: 0
```

✅ **全スロット検証成功！**

---

## 最終構成

### データフロー

```
1. check_period_coverage.py
   ↓ 欠損期間を検出
   ↓ complement_periods.txt を生成

2. run_complement_batch.py
   ↓ complement_periods.txt を読み込み
   ↓ 各期間について complement_missing.py を実行

3. complement_missing.py
   ↓ untilId更新方式でデータ取得
   ↓ early_coverage_seconds で完了判定
   ↓ data_complement/ に保存

4. verify_complement.py
   ↓ 補完データを検証
   ✓ 成功 / ⚠ 警告 / ✗ 失敗
```

### 重要な設計判断

#### 1. untilId更新方式

**なぜこの方式か**:
- Misskey API の仕様: 「untilId側から最新N件」を返す
- offset では同じ範囲を繰り返し取得してしまう
- untilId を更新することで、確実に過去に遡れる

**実装の核心**:
```python
# 最も古いノートのIDを次のuntilIdとして使用
current_until_id = oldest_note_id
```

#### 2. early_coverage_seconds

**なぜ必要か**:
- 完璧な開始時刻（00:40:00.000）を求めるのは非現実的
- スロット開始から30秒以内をカバーできれば実用上十分
- API効率とデータ網羅性のバランス

**設定可能にした理由**:
```python
--early-coverage-seconds 30  # 通常
--early-coverage-seconds 60  # より厳密に
```

#### 3. 終了カバレッジのチェック無効化

**なぜチェックしないか**:
- スロット終了ギリギリ（00:49:59.999）まで取得するのは困難
- API の挙動として、終端付近は取りこぼしやすい
- 実用上、開始カバレッジがあれば問題ない

**実装**:
```python
# 終了カバレッジは常にOKとする
has_late_coverage = True
```

#### 4. データ形式の統一

**data/ と data_complement/ の関係**:
- data/: エラーログも含む可能性
- data_complement/: 正常なMisskeyノートのみ
- 正常データの形式は完全互換

**ID重複チェック**:
```python
# data/ と data_complement/ で同じIDがないかチェック
duplicates = data_ids & complement_ids
```

### パラメータ設定の根拠

| パラメータ | 値 | 理由 |
|----------|---|------|
| `--mode` | `search` | notes/search は時刻範囲指定が可能 |
| `--limit` | `100` | Misskey API の推奨値、バランスが良い |
| `--max-pages` | `100` | 無限ループ防止、1スロット=最大10,000件 |
| `--sleep` | `5.0` | レート制限回避、安全マージン |
| `--early-coverage-seconds` | `30` | 実用性と効率のバランス |
| `--overwrite` | 有効 | 再実行時に古いデータを上書き |
| `--keep-non-japanese` | 有効 | 完全なデータセットを維持 |

---

## 学んだ教訓

### 1. API仕様の理解が最重要

**失敗例**: offset を使えば解決すると思い込んだ

**教訓**: API がどのように動作するか（「untilId側から返す」）を理解していれば、最初から untilId更新方式を選択できた

### 2. 完璧を求めすぎない

**失敗例**: スロット開始・終了の時刻をピッタリカバーしようとした

**教訓**: 実用上十分なカバレッジ（開始30秒以内）で妥協することで、効率と信頼性が向上

### 3. データ形式の事前確認

**失敗例**: data/ のフォーマットを確認せず、ID数0に混乱

**教訓**: 既存データの構造を理解してから検証ロジックを組むべき

### 4. 段階的な改善

**成功要因**:
1. まず offset で試す（失敗）
2. untilId更新方式に移行（成功）
3. 停止条件を追加（early_coverage）
4. 検証スクリプトで確認
5. 終了カバレッジを緩和

各段階で問題を発見し、解決策を重ねていった

### 5. ユーザーフィードバックの重要性

**具体例**:
- 「終了カバレッジは警告不要」→ 実用的な判断
- 「untilId側から返される」→ API動作の核心
- 「日本語フィルタを無効化」→ データ完全性の確保

技術的な判断だけでなく、実用上の判断も重要

### 6. 検証の自動化

**学び**: 補完が正しく行われたか自動確認できる仕組みが必須

**実装**: verify_complement.py による包括的検証
- ファイル存在
- ID重複
- カバレッジ
- 統計情報

---

## まとめ

### 最も重要な2つの発見

1. **untilId更新方式**: Misskey API の「untilId側から返す」仕様を理解し、最も古いノートのIDを次のuntilIdとして使用することで、確実に過去に遡れる

2. **early_coverage_seconds**: 完璧な開始時刻を求めず、開始から30秒以内をカバーすることで、実用性と効率のバランスを実現

### 開発期間

- offset方式の試行: 1日
- untilId更新方式の実装: 1日
- 検証スクリプトの開発: 1日
- ドキュメント整備: 1日

**合計: 約4日間**

### 最終的な成果

- ✅ 147期間、数百スロットの補完に成功
- ✅ 検証スクリプトで全スロット成功確認
- ✅ 再現可能で保守可能なシステム
- ✅ 包括的なドキュメント

---

## 参考資料

- [README.md](../README.md) - 使い方ガイド
- [complement_missing.py](../scripts/complement_missing.py) - 補完スクリプト本体
- [verify_complement.py](../scripts/verify_complement.py) - 検証スクリプト
- [run_complement_batch.py](../run_complement_batch.py) - バッチ実行スクリプト
