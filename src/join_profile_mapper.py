#!/usr/bin/env python3
"""
Join Profile Mapper for Social Media Analytics

This script processes user profile data for the join operation.
It tags profile data with a prefix to distinguish it from activity data.

Input format: UserID,Name,Location \t OtherProfileData
              (or whatever your actual format is)
Output format: UserID \t P:ProfileData
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

        if len(fields) >= 1:
            # In this case, it seems the user_id is mixed with other fields
            # Extract just the user_id part
            user_id_parts = fields[0].split(',')
            user_id = user_id_parts[0]  # Take just the first part

            # Combine all fields as profile data
            profile_data = fields[0]
            if len(fields) > 1:
                profile_data += "\t" + fields[1]

            # Handle skewed keys by salting
            if user_id in skewed_keys:
                # For profiles, duplicate the record for each salt value
                for i in range(NUM_SALTS):
                    salted_key = f"{user_id}_{i}"
                    print(f"{salted_key}\tP:{profile_data}")
            else:
                # Tag the record as profile data
                print(f"{user_id}\tP:{profile_data}")

    except Exception as e:
        # Log error but continue processing
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")