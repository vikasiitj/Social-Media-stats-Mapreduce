#!/usr/bin/env python3
"""
Join Reducer for Social Media Analytics

This script joins user activity data with profile data.
It performs an inner join, outputting records only when both
activity and profile data are available for a user.

Input format: UserID \t (A:ActivityData | P:ProfileData)
Output format: UserID \t ProfileData \t ActivityData
"""

import sys

current_user = None
profile_data = None
activity_data = None

# Process each line from standard input
for line in sys.stdin:
    try:
        # Split on tabs
        user_id, tagged_data = line.strip().split('\t', 1)

        # Strip any salt value from user_id (for skewed keys)
        if '_' in user_id:
            user_id = user_id.split('_')[0]

        # If this is a new user, process the previous user
        if user_id != current_user:
            # Output joined data for previous user if we have both profile and activity
            if current_user is not None and profile_data is not None and activity_data is not None:
                print(f"{current_user}\t{profile_data}\t{activity_data}")

            # Reset for new user
            current_user = user_id
            profile_data = None
            activity_data = None

        # Store data based on tag
        tag = tagged_data[0]
        data = tagged_data[2:]  # Skip the tag and colon

        if tag == 'P':
            profile_data = data
        elif tag == 'A':
            activity_data = data

    except Exception as e:
        # Log error but continue processing
        sys.stderr.write(f"Error processing line: {line.strip()}, Error: {str(e)}\n")

# Process the last user
if current_user is not None and profile_data is not None and activity_data is not None:
    print(f"{current_user}\t{profile_data}\t{activity_data}")