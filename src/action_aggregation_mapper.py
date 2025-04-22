#!/usr/bin/env python3
"""
Action Aggregation Mapper for Social Media Analytics

This script aggregates user actions (posts, likes, comments, shares) by user ID.
It uses in-memory aggregation to reduce the amount of intermediate data.

Input format: UserID \t Timestamp \t ActionType \t ContentID \t Metadata
Output format: UserID,SortKey \t post_count,like_count,comment_count,share_count
"""

import sys
from collections import defaultdict

# In-memory aggregation for user actions
user_action_counts = defaultdict(lambda: {'post': 0, 'like': 0, 'comment': 0, 'share': 0})

# Process each line from standard input
for line in sys.stdin:
    try:
        # Split on tabs
        fields = line.strip().split('\t')

        if len(fields) >= 3:
            user_id = fields[0]
            action_type = fields[2].lower()

            # Count each type of action
            if action_type in ['post', 'like', 'comment', 'share']:
                user_action_counts[user_id][action_type] += 1

    except Exception as e:
        # Log error but continue processing
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

# Emit aggregated counts for each user
for user_id, counts in user_action_counts.items():
    # Create a sort key from post count (padded for lexicographic sorting)
    # We use 10000-count to sort in descending order
    sort_key = f"{10000 - counts['post']:05d}"

    # Composite key: user_id,sort_key
    key = f"{user_id},{sort_key}"

    # Value contains all counts
    value = f"{counts['post']},{counts['like']},{counts['comment']},{counts['share']}"

    print(f"{key}\t{value}")