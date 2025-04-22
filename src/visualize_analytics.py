"""
Social Media Analytics Visualization

This script generates visualizations from the MapReduce workflow output.
It creates several charts to provide insights into user activity and trending content.

Usage:
    python visualize_analytics.py --input-dir output
"""

import os
import sys
import argparse

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
from collections import defaultdict


def parse_user_activity(file_path):
    """
    Parse the user activity data.

    Args:
        file_path: Path to the user activity output file

    Returns:
        DataFrame with user activity data
    """
    print(f"Parsing user activity data from: {file_path}")

    users = []
    posts = []
    likes = []
    comments = []
    shares = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    parts = line.strip().split('\t')
                    if len(parts) >= 2:
                        user_id = parts[0]
                        activity_data = parts[1]

                        # Parse activity counts
                        activity_parts = activity_data.split(',')
                        activity_dict = {}
                        for part in activity_parts:
                            key_value = part.split(':')
                            if len(key_value) == 2:
                                activity_dict[key_value[0]] = int(key_value[1])

                        users.append(user_id)
                        posts.append(activity_dict.get('posts', 0))
                        likes.append(activity_dict.get('likes', 0))
                        comments.append(activity_dict.get('comments', 0))
                        shares.append(activity_dict.get('shares', 0))
                except Exception as e:
                    print(f"Error parsing line: {line.strip()}, Error: {str(e)}")
    except Exception as e:
        print(f"Error reading file: {str(e)}")

    # Create DataFrame
    data = {
        'user_id': users,
        'posts': posts,
        'likes': likes,
        'comments': comments,
        'shares': shares,
        'total_activity': [p + l + c + s for p, l, c, s in zip(posts, likes, comments, shares)]
    }

    df = pd.DataFrame(data)
    print(f"Parsed {len(df)} user activity records")
    return df


def parse_trending_content(file_path):
    """
    Parse the trending content data.

    Args:
        file_path: Path to the trending content output file

    Returns:
        DataFrame with trending content data
    """
    print(f"Parsing trending content data from: {file_path}")

    content_ids = []
    engagement_scores = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    parts = line.strip().split('\t')
                    if len(parts) >= 2:
                        content_id = parts[0]
                        engagement = int(parts[1])

                        content_ids.append(content_id)
                        engagement_scores.append(engagement)
                except Exception as e:
                    print(f"Error parsing line: {line.strip()}, Error: {str(e)}")
    except Exception as e:
        print(f"Error reading file: {str(e)}")

    # Create DataFrame
    data = {
        'content_id': content_ids,
        'engagement': engagement_scores
    }

    df = pd.DataFrame(data)
    print(f"Parsed {len(df)} trending content records")
    return df


def create_activity_distribution_chart(df, output_path):
    """
    Create a chart showing the distribution of different activity types.

    Args:
        df: DataFrame with user activity data
        output_path: Path to save the chart
    """
    print("Creating activity distribution chart...")

    # Calculate totals for each activity type
    totals = {
        'Posts': df['posts'].sum(),
        'Likes': df['likes'].sum(),
        'Comments': df['comments'].sum(),
        'Shares': df['shares'].sum()
    }

    # Create pie chart
    plt.figure(figsize=(10, 8))
    plt.pie(
        totals.values(),
        labels=totals.keys(),
        autopct='%1.1f%%',
        startangle=90,
        shadow=True,
        explode=[0.05, 0.05, 0.05, 0.05]
    )
    plt.title('Distribution of Social Media Activities', fontsize=16)
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle

    # Save chart
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"Activity distribution chart saved to: {output_path}")


def create_top_users_chart(df, output_path, top_n=20):
    """
    Create a chart showing the top N most active users.

    Args:
        df: DataFrame with user activity data
        output_path: Path to save the chart
        top_n: Number of top users to show
    """
    print(f"Creating top {top_n} users chart...")

    # Get top N users by total activity
    top_users = df.nlargest(top_n, 'total_activity')

    # Create horizontal bar chart
    plt.figure(figsize=(12, 10))

    # Create stacked bar chart
    activities = ['posts', 'comments', 'shares', 'likes']
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']

    bottom = np.zeros(len(top_users))
    for i, activity in enumerate(activities):
        plt.barh(
            top_users['user_id'],
            top_users[activity],
            left=bottom,
            color=colors[i],
            label=activity.capitalize()
        )
        bottom += top_users[activity].values

    plt.xlabel('Activity Count', fontsize=12)
    plt.ylabel('User ID', fontsize=12)
    plt.title(f'Top {top_n} Most Active Users', fontsize=16)
    plt.legend(loc='upper right')
    plt.grid(axis='x', linestyle='--', alpha=0.7)

    # Ensure user IDs are readable
    plt.yticks(fontsize=8)

    # Save chart
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"Top users chart saved to: {output_path}")


def create_trending_content_chart(df, output_path, top_n=20):
    """
    Create a chart showing the top N trending content items.

    Args:
        df: DataFrame with trending content data
        output_path: Path to save the chart
        top_n: Number of top content items to show
    """
    print(f"Creating top {top_n} trending content chart...")

    # Get top N content items by engagement
    top_content = df.nlargest(top_n, 'engagement')

    # Create horizontal bar chart
    plt.figure(figsize=(12, 10))

    # Use a colormap for the bars
    colors = plt.cm.viridis(np.linspace(0, 1, len(top_content)))

    plt.barh(
        top_content['content_id'],
        top_content['engagement'],
        color=colors
    )

    plt.xlabel('Engagement Score (Likes + Shares)', fontsize=12)
    plt.ylabel('Content ID', fontsize=12)
    plt.title(f'Top {top_n} Trending Content Items', fontsize=16)
    plt.grid(axis='x', linestyle='--', alpha=0.7)

    # Ensure content IDs are readable
    plt.yticks(fontsize=8)

    # Save chart
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"Trending content chart saved to: {output_path}")


def create_user_activity_heatmap(df, output_path):
    """
    Create a heatmap showing correlations between different activity types.

    Args:
        df: DataFrame with user activity data
        output_path: Path to save the chart
    """
    print("Creating user activity correlation heatmap...")

    # Calculate correlation matrix
    correlation_matrix = df[['posts', 'likes', 'comments', 'shares']].corr()

    # Create heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(
        correlation_matrix,
        annot=True,
        cmap='coolwarm',
        vmin=-1,
        vmax=1,
        center=0,
        linewidths=.5
    )
    plt.title('Correlation Between Different Activity Types', fontsize=16)

    # Save chart
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"Activity correlation heatmap saved to: {output_path}")


def create_engagement_distribution_chart(df, output_path):
    """
    Create a histogram showing the distribution of engagement scores.

    Args:
        df: DataFrame with trending content data
        output_path: Path to save the chart
    """
    print("Creating engagement distribution chart...")

    plt.figure(figsize=(12, 8))

    # Create histogram with KDE
    sns.histplot(
        df['engagement'],
        kde=True,
        bins=50,
        color='skyblue'
    )

    plt.xlabel('Engagement Score', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.title('Distribution of Content Engagement Scores', fontsize=16)
    plt.grid(linestyle='--', alpha=0.7)

    # Add vertical line for mean and median
    mean_engagement = df['engagement'].mean()
    median_engagement = df['engagement'].median()

    plt.axvline(mean_engagement, color='red', linestyle='--', label=f'Mean: {mean_engagement:.1f}')
    plt.axvline(median_engagement, color='green', linestyle='-.', label=f'Median: {median_engagement:.1f}')
    plt.legend()

    # Save chart
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"Engagement distribution chart saved to: {output_path}")


def create_visualizations(input_dir, output_dir):
    """
    Create all visualizations from the analytics data.

    Args:
        input_dir: Directory containing the analytics output files
        output_dir: Directory to save the visualizations
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Define input file paths
    user_activity_path = os.path.join(input_dir, 'user_activity.txt')
    trending_content_path = os.path.join(input_dir, 'trending_content.txt')

    # Define output file paths
    activity_dist_path = os.path.join(output_dir, 'activity_distribution.png')
    top_users_path = os.path.join(output_dir, 'top_users.png')
    trending_content_path_out = os.path.join(output_dir, 'trending_content.png')
    activity_heatmap_path = os.path.join(output_dir, 'activity_correlation.png')
    engagement_dist_path = os.path.join(output_dir, 'engagement_distribution.png')

    # Parse data
    user_activity_df = parse_user_activity(user_activity_path)
    trending_content_df = parse_trending_content(trending_content_path)

    # Create visualizations
    if not user_activity_df.empty:
        import numpy as np  # Import numpy for stacked bar chart

        create_activity_distribution_chart(user_activity_df, activity_dist_path)
        create_top_users_chart(user_activity_df, top_users_path)
        create_user_activity_heatmap(user_activity_df, activity_heatmap_path)
    else:
        print("No user activity data to visualize")

    if not trending_content_df.empty:
        import numpy as np  # Import numpy for color mapping

        create_trending_content_chart(trending_content_df, trending_content_path_out)
        create_engagement_distribution_chart(trending_content_df, engagement_dist_path)
    else:
        print("No trending content data to visualize")

    print("Visualization creation complete!")


def main():
    parser = argparse.ArgumentParser(description="Social Media Analytics Visualization")
    parser.add_argument('--input-dir', default='output', help='Input directory containing analytics output files')
    parser.add_argument('--output-dir', default='visualizations', help='Output directory for visualizations')
    args = parser.parse_args()

    try:
        create_visualizations(args.input_dir, args.output_dir)
    except Exception as e:
        print(f"Error creating visualizations: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())