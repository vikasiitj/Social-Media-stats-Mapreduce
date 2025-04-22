#!/usr/bin/env python3
"""
Skew Detection Utility for MapReduce Workflow

This script analyzes the distribution of keys in a dataset to identify potential
data skew issues. It's used to optimize the join operation in the social media
analytics MapReduce workflow.

Usage:
    python3 skew_detection.py < input_data.txt > skew_analysis.json

The input should be lines of tab-separated key-value pairs.
The output is a JSON object with skew statistics.
"""

import sys
import json
from collections import Counter
import numpy as np


def analyze_key_distribution(lines, skew_threshold_factor=0.01):
    """
    Analyze the distribution of keys to identify skew.

    Args:
        lines: Iterable of input lines (tab-separated key-value pairs)
        skew_threshold_factor: Fraction of total records to consider a key skewed

    Returns:
        Dictionary with skew analysis statistics
    """
    # Count occurrences of each key
    key_counter = Counter()
    total_records = 0

    for line in lines:
        total_records += 1
        try:
            key = line.strip().split('\t')[0]

            # If using composite key (from action aggregation), extract user ID
            if ',' in key:
                key = key.split(',')[0]

            key_counter[key] += 1
        except Exception as e:
            sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

    # Calculate statistics
    if not key_counter:
        return {
            "total_records": 0,
            "unique_keys": 0,
            "skew_threshold": 0,
            "skewed_keys": [],
            "top_keys": []
        }

    unique_keys = len(key_counter)
    avg_records_per_key = total_records / unique_keys if unique_keys > 0 else 0

    # Calculate skew threshold (keys with significantly more records than average)
    skew_threshold = max(
        skew_threshold_factor * total_records,  # Absolute threshold
        avg_records_per_key * 5  # Relative threshold
    )

    # Find skewed keys
    skewed_keys = [key for key, count in key_counter.items() if count > skew_threshold]

    # Get top keys by frequency
    top_keys = key_counter.most_common(10)

    # Calculate key distribution statistics
    counts = np.array(list(key_counter.values()))

    return {
        "total_records": total_records,
        "unique_keys": unique_keys,
        "average_records_per_key": float(avg_records_per_key),
        "skew_threshold": float(skew_threshold),
        "skewed_keys": skewed_keys,
        "skewed_keys_count": len(skewed_keys),
        "top_keys": top_keys,
        "distribution_stats": {
            "min": int(np.min(counts)),
            "max": int(np.max(counts)),
            "median": float(np.median(counts)),
            "mean": float(np.mean(counts)),
            "std_dev": float(np.std(counts)),
            "p90": float(np.percentile(counts, 90)),
            "p95": float(np.percentile(counts, 95)),
            "p99": float(np.percentile(counts, 99))
        }
    }


def main():
    """Main entry point."""
    try:
        # Read from standard input
        lines = sys.stdin.readlines()

        # Analyze key distribution
        analysis = analyze_key_distribution(lines)

        # Output as JSON
        print(json.dumps(analysis, indent=2))

    except Exception as e:
        error_output = {
            "error": str(e),
            "skewed_keys": []
        }
        print(json.dumps(error_output, indent=2))
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())