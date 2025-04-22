#!/usr/bin/env python3
"""
Data Cleansing Mapper for Social Media Analytics

This script parses and validates records from social_media_logs.txt,
extracting Timestamp, UserID, ActionType, ContentID, and Metadata.
It filters out malformed records and tracks discarded records using counters.

Input format: Tab-separated record from social_media_logs.txt
Output format: UserID \t Timestamp \t ActionType \t ContentID \t Metadata
"""

import sys
import json
import re
from datetime import datetime

# Counter for tracking discarded records
discarded_records = {
    'invalid_timestamp': 0,
    'malformed_json': 0,
    'missing_fields': 0,
    'total_discarded': 0
}

# Timestamp validation pattern (YYYY-MM-DDThh:mm:ss)
timestamp_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$')


def validate_timestamp(timestamp):
    """
    Validate the timestamp format.

    Args:
        timestamp: String timestamp to validate

    Returns:
        True if valid, False otherwise
    """
    if not timestamp_pattern.match(timestamp):
        return False

    try:
        # Strip the 'Z' if present before parsing
        if timestamp.endswith('Z'):
            timestamp = timestamp[:-1]
        datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S')
        return True
    except ValueError:
        return False


def validate_json(json_str):
    """
    Validate the JSON metadata.

    Args:
        json_str: JSON string to validate

    Returns:
        True if valid, False otherwise
    """
    try:
        json.loads(json_str)
        return True
    except json.JSONDecodeError:
        return False


# Process each line from standard input
for line in sys.stdin:
    try:
        # Remove leading/trailing whitespace and split on tabs
        fields = line.strip().split('\t')

        # Check if record has all required fields
        if len(fields) < 5:
            discarded_records['missing_fields'] += 1
            discarded_records['total_discarded'] += 1
            continue

        timestamp, user_id, action_type, content_id, metadata_json = fields[:5]

        # Validate timestamp format
        if not validate_timestamp(timestamp):
            discarded_records['invalid_timestamp'] += 1
            discarded_records['total_discarded'] += 1
            continue

        # Validate JSON metadata
        if not validate_json(metadata_json):
            discarded_records['malformed_json'] += 1
            discarded_records['total_discarded'] += 1
            continue

        # Emit valid record
        print(f"{user_id}\t{timestamp}\t{action_type}\t{content_id}\t{metadata_json}")

    except Exception as e:
        # Count any other errors as discarded records
        discarded_records['total_discarded'] += 1
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

# Output counters as a special record with reserved key
for counter_name, counter_value in discarded_records.items():
    sys.stderr.write(f"reporter:counter:DataQuality,{counter_name},{counter_value}\n")