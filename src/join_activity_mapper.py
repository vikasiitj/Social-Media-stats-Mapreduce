#!/usr/bin/env python3
"""
Join Activity Mapper for Social Media Analytics

This script processes user activity data for the join operation.
It tags activity data with a prefix to distinguish it from profile data.

Input format: UserID \t posts:N,likes:N,comments:N,shares:N
Output format: UserID \t A:posts:N,likes:N,comments:N,shares:N
"""

import sys
import os

# Get list of skewed keys (if any)
skewed_keys_str = os.environ.get('skewed.keys', '')
skewed_keys = set(skewed_keys_str.split(',')) if skewed_keys_str else set()

# Number of salt values to use for skewed keys
NUM_SALTS = 10

# Process each line from standard input
for line in sys.stdin:
    try:
        # Split on tabs
        fields = line.strip().split('\t', 1)

        if len(fields) >= 2:
            user_id = fields[0]
            activity_data = fields[1]

            # Handle skewed keys by salting
            if user_id in skewed_keys:
                # Emit multiple records with salted keys
                for i in range(NUM_SALTS):
                    salted_key = f"{user_id}_{i}"
                    print(f"{salted_key}\tA:{activity_data}")
            else:
                # Tag the record as activity data
                print(f"{user_id}\tA:{activity_data}")

    except Exception as e:
        # Log error but continue processing
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")