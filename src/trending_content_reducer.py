#!/usr/bin/env python3
"""
Trending Content Reducer for Social Media Analytics

This script determines a threshold for trending content and identifies
content items that exceed this threshold. It can use a fixed threshold
or calculate one dynamically based on the data distribution.

Input format: ContentID \t EngagementCount
Output format: ContentID \t EngagementCount
"""

import sys
import os
import numpy as np

# Get threshold from environment variable or use dynamic calculation
threshold = int(os.environ.get('TRENDING_THRESHOLD', -1))

# Collect all engagement counts
all_engagements = []
content_data = []

# First pass: collect all data
for line in sys.stdin:
    try:
        # Split on tabs
        content_id, engagement = line.strip().split('\t')
        engagement = int(engagement)

        # Store for later processing
        all_engagements.append(engagement)
        content_data.append((content_id, engagement))

    except Exception as e:
        # Log error but continue processing
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

# Determine threshold if not provided
if threshold < 0:
    # Calculate threshold as 90th percentile
    if all_engagements:
        threshold = np.percentile(all_engagements, 90)
    else:
        threshold = 0

# Report the threshold used
sys.stderr.write(f"reporter:counter:TrendingStats,ThresholdUsed,{int(threshold)}\n")

# Second pass: emit trending content
for content_id, engagement in content_data:
    if engagement >= threshold:
        print(f"{content_id}\t{engagement}")