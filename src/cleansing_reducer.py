#!/usr/bin/env python3
"""
Data Cleansing Reducer for Social Media Analytics

This script simply passes through the valid records that have been filtered
by the cleansing mapper.

Input format: UserID \t Timestamp \t ActionType \t ContentID \t Metadata
Output format: UserID \t Timestamp \t ActionType \t ContentID \t Metadata
"""

import sys

# Process each line from standard input
for line in sys.stdin:
    # Simply pass through the valid records
    print(line.strip())