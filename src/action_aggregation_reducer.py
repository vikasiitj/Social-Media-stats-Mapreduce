#!/usr/bin/env python3
"""
Action Aggregation Reducer for Social Media Analytics

This script processes the aggregated user actions and formats them.
It expects a composite key sorted by post count (descending).

Input format: UserID,SortKey \t post_count,like_count,comment_count,share_count
Output format: UserID \t posts:N,likes:N,comments:N,shares:N
"""

import sys

current_user = None

# Process each line from standard input
for line in sys.stdin:
    try:
        # Split on tabs
        key, value = line.strip().split('\t')

        # Extract user_id from composite key (format: user_id,padded_post_count)
        user_id = key.split(',')[0]

        # If this is a new user, emit the record
        if user_id != current_user:
            current_user = user_id

            # Parse counts from value
            post_count, like_count, comment_count, share_count = map(int, value.split(','))

            # Format output
            output = f"{user_id}\tposts:{post_count},likes:{like_count},comments:{comment_count},shares:{share_count}"
            print(output)

    except Exception as e:
        # Log error but continue processing
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")