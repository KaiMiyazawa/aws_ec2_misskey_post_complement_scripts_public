#!/usr/bin/env python3
"""
data/とdata_complement/におけるリプライ・リノート・通常投稿の割合を分析するスクリプト
"""

import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, Tuple


def analyze_post_types(jsonl_file: Path) -> Tuple[int, int, int, int]:
    """
    JSONLファイル内の投稿タイプを分析

    Returns:
        Tuple[int, int, int, int]: (total, replies, renotes, original_posts)
    """
    total = 0
    replies = 0
    renotes = 0
    original_posts = 0

    try:
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    post = json.loads(line.strip())
                    total += 1

                    # replyId があればリプライ
                    has_reply_id = post.get('replyId') is not None
                    # renoteId があればリノート
                    has_renote_id = post.get('renoteId') is not None

                    if has_reply_id:
                        replies += 1
                    elif has_renote_id:
                        # リプライではなくリノートのみの場合
                        renotes += 1
                    else:
                        # リプライでもリノートでもない通常投稿
                        original_posts += 1

                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    continue

    except Exception as e:
        print(f"Error reading {jsonl_file}: {e}")

    return total, replies, renotes, original_posts


def analyze_directory(data_dir: Path, max_files: int = None) -> Dict[str, int]:
    """
    データディレクトリ全体を分析

    Args:
        data_dir: 分析対象のディレクトリ
        max_files: 処理する最大ファイル数（Noneの場合は全ファイル）

    Returns:
        Dict with total, replies, renotes, original_posts counts
    """
    stats = {
        'total': 0,
        'replies': 0,
        'renotes': 0,
        'original_posts': 0
    }

    if not data_dir.exists():
        print(f"Warning: {data_dir} does not exist")
        return stats

    # JSONLファイルを収集
    jsonl_files = list(data_dir.glob("*/*/*/*/*.jsonl"))

    if max_files:
        jsonl_files = jsonl_files[:max_files]

    print(f"Analyzing {len(jsonl_files)} files in {data_dir.name}/...")

    processed = 0
    for jsonl_file in jsonl_files:
        total, replies, renotes, original = analyze_post_types(jsonl_file)

        stats['total'] += total
        stats['replies'] += replies
        stats['renotes'] += renotes
        stats['original_posts'] += original

        processed += 1
        if processed % 500 == 0:
            print(f"  Processed {processed}/{len(jsonl_files)} files...")

    return stats


def print_statistics(label: str, stats: Dict[str, int]):
    """統計情報を表示"""
    total = stats['total']
    replies = stats['replies']
    renotes = stats['renotes']
    original = stats['original_posts']

    print(f"\n{'='*60}")
    print(f"{label}")
    print(f"{'='*60}")
    print(f"Total posts:        {total:>12,} (100.0%)")

    if total > 0:
        print(f"├─ Replies:         {replies:>12,} ({replies/total*100:>5.2f}%)")
        print(f"├─ Renotes:         {renotes:>12,} ({renotes/total*100:>5.2f}%)")
        print(f"└─ Original posts:  {original:>12,} ({original/total*100:>5.2f}%)")

    print(f"{'='*60}\n")


def main():
    """メイン処理"""
    # プロジェクトルートの取得
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    # データディレクトリのパス
    data_original_dir = project_root / "data"
    data_complement_dir = project_root / "data_complement"

    # data/ の分析
    print("\n" + "="*60)
    print("Analyzing data/ (Streaming API)")
    print("="*60)
    stats_original = analyze_directory(data_original_dir)
    print_statistics("data/ (Streaming API)", stats_original)

    # data_complement/ の分析
    print("\n" + "="*60)
    print("Analyzing data_complement/ (notes/search API)")
    print("="*60)
    stats_complement = analyze_directory(data_complement_dir)
    print_statistics("data_complement/ (notes/search API)", stats_complement)

    # 比較サマリー
    print("\n" + "="*60)
    print("COMPARISON SUMMARY")
    print("="*60)

    if stats_original['total'] > 0:
        orig_reply_ratio = stats_original['replies'] / stats_original['total'] * 100
        orig_renote_ratio = stats_original['renotes'] / stats_original['total'] * 100
        orig_original_ratio = stats_original['original_posts'] / stats_original['total'] * 100

        print(f"\ndata/ (Streaming API):")
        print(f"  Reply ratio:    {orig_reply_ratio:>6.2f}%")
        print(f"  Renote ratio:   {orig_renote_ratio:>6.2f}%")
        print(f"  Original ratio: {orig_original_ratio:>6.2f}%")

    if stats_complement['total'] > 0:
        comp_reply_ratio = stats_complement['replies'] / stats_complement['total'] * 100
        comp_renote_ratio = stats_complement['renotes'] / stats_complement['total'] * 100
        comp_original_ratio = stats_complement['original_posts'] / stats_complement['total'] * 100

        print(f"\ndata_complement/ (notes/search API):")
        print(f"  Reply ratio:    {comp_reply_ratio:>6.2f}%")
        print(f"  Renote ratio:   {comp_renote_ratio:>6.2f}%")
        print(f"  Original ratio: {comp_original_ratio:>6.2f}%")

    print("\n" + "="*60 + "\n")


if __name__ == "__main__":
    main()
