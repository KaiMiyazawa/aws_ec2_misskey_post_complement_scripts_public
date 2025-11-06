#!/usr/bin/env python3
"""
時系列での投稿数推移を可視化するスクリプト

data/のデータ（青色）とdata_complement/のデータ（赤色）を
別々にプロットして、データ補完の効果を視覚化します。
"""

import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from typing import Dict, List, Tuple

# 日本語フォント設定（必要に応じて）
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def count_posts_in_jsonl(file_path: Path) -> int:
    """JSONLファイル内の投稿数をカウント"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0


def collect_timeseries_data(data_dir: Path) -> Dict[datetime, int]:
    """
    データディレクトリから時系列データを収集

    Returns:
        Dict[datetime, int]: 日時ごとの投稿数
    """
    timeseries = defaultdict(int)

    if not data_dir.exists():
        print(f"Warning: {data_dir} does not exist")
        return timeseries

    # YYYY/MM/DD/HH/*.jsonl の構造を走査
    for jsonl_file in data_dir.glob("*/*/*/*/*.jsonl"):
        # ファイル名から日時を抽出: 2025-08-01_00-00.jsonl
        try:
            filename = jsonl_file.stem  # 拡張子を除く
            # "2025-08-01_00-00" -> datetime
            date_str, time_str = filename.split('_')
            hour, minute = time_str.split('-')
            dt = datetime.strptime(f"{date_str} {hour}:{minute}", "%Y-%m-%d %H:%M")

            # 投稿数をカウント
            post_count = count_posts_in_jsonl(jsonl_file)
            timeseries[dt] += post_count

        except Exception as e:
            print(f"Error processing {jsonl_file}: {e}")
            continue

    return timeseries


def plot_timeseries(
    data_original: Dict[datetime, int],
    data_complement: Dict[datetime, int],
    output_path: str = "output/timeseries_posts.png"
):
    """
    時系列データをプロット

    Args:
        data_original: data/からの投稿数データ（青色）
        data_complement: data_complement/からの投稿数データ（赤色）
        output_path: 出力画像のパス
    """
    fig, ax = plt.subplots(figsize=(14, 6))

    # data/ のデータ（青色）
    if data_original:
        times_orig = sorted(data_original.keys())
        counts_orig = [data_original[t] for t in times_orig]
        ax.plot(times_orig, counts_orig, 'b-', alpha=0.7, linewidth=1, label='data/ (original)')

    # data_complement/ のデータ（赤色）
    if data_complement:
        times_comp = sorted(data_complement.keys())
        counts_comp = [data_complement[t] for t in times_comp]
        ax.plot(times_comp, counts_comp, 'r-', alpha=0.7, linewidth=1, label='data_complement/ (補完データ)')

    # グラフの装飾
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Number of Posts', fontsize=12)
    ax.set_title('Time Series of Misskey Posts', fontsize=14, fontweight='bold')
    ax.set_ylim(0, 2000)  # y軸を0〜2000に制限
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)

    # 日付フォーマットの設定
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    plt.xticks(rotation=45, ha='right')

    # レイアウト調整
    plt.tight_layout()

    # 出力ディレクトリの作成
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    # 保存
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_path}")

    plt.close()


def print_statistics(data_original: Dict[datetime, int], data_complement: Dict[datetime, int]):
    """データの統計情報を出力"""
    print("\n=== Data Statistics ===")

    if data_original:
        total_orig = sum(data_original.values())
        date_count_orig = len(data_original)
        print(f"\ndata/ (Original):")
        print(f"  Total posts: {total_orig:,}")
        print(f"  Time points: {date_count_orig}")
        print(f"  Date range: {min(data_original.keys())} to {max(data_original.keys())}")
        if date_count_orig > 0:
            print(f"  Average posts per time point: {total_orig / date_count_orig:.1f}")

    if data_complement:
        total_comp = sum(data_complement.values())
        date_count_comp = len(data_complement)
        print(f"\ndata_complement/ (Complement):")
        print(f"  Total posts: {total_comp:,}")
        print(f"  Time points: {date_count_comp}")
        print(f"  Date range: {min(data_complement.keys())} to {max(data_complement.keys())}")
        if date_count_comp > 0:
            print(f"  Average posts per time point: {total_comp / date_count_comp:.1f}")

    print("\n" + "="*50 + "\n")


def save_timeseries_data(
    data_original: Dict[datetime, int],
    data_complement: Dict[datetime, int],
    output_path: str
):
    """
    時系列データをCSVファイルに保存

    Args:
        data_original: data/からの投稿数データ
        data_complement: data_complement/からの投稿数データ
        output_path: 出力CSVファイルのパス
    """
    # 全ての時刻を収集
    all_times = sorted(set(data_original.keys()) | set(data_complement.keys()))

    # 出力ディレクトリの作成
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    # CSVファイルに書き出し
    with open(output_path, 'w', encoding='utf-8') as f:
        # ヘッダー
        f.write("datetime,data_original,data_complement,total\n")

        # データ行
        for dt in all_times:
            count_orig = data_original.get(dt, 0)
            count_comp = data_complement.get(dt, 0)
            total = count_orig + count_comp
            f.write(f"{dt.isoformat()},{count_orig},{count_comp},{total}\n")

    print(f"Time series data saved to: {output_path}")


def main():
    """メイン処理"""
    # プロジェクトルートの取得
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # データディレクトリのパス
    data_original_dir = project_root / "data"
    data_complement_dir = project_root / "data_complement"

    print("Collecting time series data from data/...")
    data_original = collect_timeseries_data(data_original_dir)
    print(f"Found {len(data_original)} time points in data/")

    print("\nCollecting time series data from data_complement/...")
    data_complement = collect_timeseries_data(data_complement_dir)
    print(f"Found {len(data_complement)} time points in data_complement/")

    # 統計情報の表示
    print_statistics(data_original, data_complement)

    # 時系列データをCSVに保存
    csv_output_path = project_root / "output" / "timeseries_data.csv"
    print(f"Saving time series data to CSV...")
    save_timeseries_data(data_original, data_complement, str(csv_output_path))

    # プロット作成
    output_path = project_root / "output" / "timeseries_posts.png"
    print(f"Creating plot...")
    plot_timeseries(data_original, data_complement, str(output_path))

    print("\nDone!")


if __name__ == "__main__":
    main()
