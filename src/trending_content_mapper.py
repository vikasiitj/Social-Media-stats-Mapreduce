#!/usr/bin/env python3
"""
Trending Content Mapper for Social Media Analytics

This script identifies trending content by counting likes and shares per content item.
It uses in-memory aggregation to reduce the amount of intermediate data.

Input format: UserID \t Timestamp \t ActionType \t ContentID \t Metadata
Output format: ContentID \t EngagementCount
"""

import sys
from collections import defaultdict

# In-memory aggregation for content engagement
content_engagement = defaultdict(int)

# Process each line from standard input
for line in sys.stdin:
    try:
        # Split on tabs
        fields = line.strip().split('\t')

        if len(fields) >= 4:
            content_id = fields[3]
            action_type = fields[2].lower()

            # Only consider likes and shares for trending metric
            if action_type in ['like', 'share']:
                content_engagement[content_id] += 1

    except Exception as e:
        # Log error but continue processing
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

# Emit aggregated engagement for each content item
for content_id, engagement in content_engagement.items():
    print(f"{content_id}\t{engagement}")