# Add this code to a temporary script to check for overlapping user IDs
import os
# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Go up one directory to the project root
project_root = os.path.dirname(script_dir)

# Create absolute paths relative to the project root
user_activity_path = os.path.join(project_root, 'output', 'user_activity.txt')
user_profiles_path = os.path.join(project_root, 'data', 'user_profiles.txt')

# Extract user IDs from user activity data
activity_user_ids = set()
with open(user_activity_path, 'r', encoding='utf-8') as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) >= 1:
            activity_user_ids.add(parts[0])

# Extract user IDs from profiles data
profile_user_ids = set()
with open(user_profiles_path, 'r', encoding='utf-8') as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) >= 1:
            profile_user_ids.add(parts[0])

# Check for overlap
overlap = activity_user_ids.intersection(profile_user_ids)

print(f"User IDs in activity data: {len(activity_user_ids)}")
print(f"User IDs in profile data: {len(profile_user_ids)}")
print(f"Overlapping user IDs: {len(overlap)}")
print(f"Sample of activity user IDs: {list(activity_user_ids)[:5]}")
print(f"Sample of profile user IDs: {list(profile_user_ids)[:5]}")