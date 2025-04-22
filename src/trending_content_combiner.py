#!/usr/bin/env python3
"""
Trending Content Combiner for Social Media Analytics

This script acts as a combiner to locally aggregate engagement counts.
It reduces the amount of data transferred in the shuffle phase.

Input format: ContentID \t EngagementCount
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
        content_id, engagement = line.strip().split('\t')
        content_engagement[content_id] += int(engagement)

    except Exception as e:
        # Log error but continue processing
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

# Emit combined engagement for each content item
for content_id, engagement in content_engagement.items():
    print(f"{content_id}\t{engagement}")