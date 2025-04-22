"""
Social Media Analytics Dashboard

This script creates an interactive dashboard for exploring the social media analytics data.
It uses Dash and Plotly to provide an interactive web interface.

Usage:
    python analytics_dashboard.py
"""

import os

import dash
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html, Input, Output
from scipy import stats

# Define paths
DATA_DIR = 'output'
USER_ACTIVITY_PATH = os.path.join(DATA_DIR, 'user_activity.txt')
TRENDING_CONTENT_PATH = os.path.join(DATA_DIR, 'trending_content.txt')


# Function to parse data files
def parse_user_activity(file_path):
    """Parse the user activity data file."""
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

    return pd.DataFrame(data)


def parse_trending_content(file_path):
    """Parse the trending content data file."""
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

    return pd.DataFrame(data)


# Load data
print("Loading user activity data...")
try:
    user_activity_df = parse_user_activity(USER_ACTIVITY_PATH)
    print(f"Loaded {len(user_activity_df)} user activity records")
except Exception as e:
    print(f"Error loading user activity data: {str(e)}")
    # Create empty DataFrame if file not found or error occurs
    user_activity_df = pd.DataFrame(columns=['user_id', 'posts', 'likes', 'comments', 'shares', 'total_activity'])

print("Loading trending content data...")
try:
    trending_content_df = parse_trending_content(TRENDING_CONTENT_PATH)
    print(f"Loaded {len(trending_content_df)} trending content records")
except Exception as e:
    print(f"Error loading trending content data: {str(e)}")
    # Create empty DataFrame if file not found or error occurs
    trending_content_df = pd.DataFrame(columns=['content_id', 'engagement'])

# Check if data is available for visualization
has_user_data = not user_activity_df.empty
has_content_data = not trending_content_df.empty

if not has_user_data and not has_content_data:
    print("Warning: No data available for visualization. The dashboard will show empty charts.")

# Initialize the Dash app
app = dash.Dash(__name__, title='Social Media Analytics Dashboard')

# Define CSS styles
styles = {
    'container': {
        'margin': '20px',
        'fontFamily': 'Arial'
    },
    'header': {
        'textAlign': 'center',
        'marginBottom': '30px',
        'color': '#2c3e50'
    },
    'tab_container': {
        'marginBottom': '20px'
    },
    'chart_container': {
        'marginTop': '20px',
        'padding': '10px',
        'border': '1px solid #ddd',
        'borderRadius': '5px',
        'backgroundColor': '#f9f9f9'
    },
    'half_width': {
        'width': '49%',
        'display': 'inline-block',
        'verticalAlign': 'top'
    },
    'control_panel': {
        'marginBottom': '15px',
        'padding': '10px',
        'backgroundColor': '#f0f0f0',
        'borderRadius': '5px'
    },
    'footer': {
        'textAlign': 'center',
        'padding': '10px',
        'marginTop': '30px',
        'borderTop': '1px solid #ddd',
        'color': '#7f8c8d'
    }
}

# Define the layout
app.layout = html.Div([
    # Header
    html.H1('Social Media Analytics Dashboard', style=styles['header']),

    # Info panel
    html.Div([
        html.Div([
            html.H4('Data Summary'),
            html.P(f"User Activity Records: {len(user_activity_df)}"),
            html.P(f"Trending Content Items: {len(trending_content_df)}")
        ], style=styles['chart_container'])
    ]),

    # Tabs for different views
    html.Div([
        dcc.Tabs([
            # Tab 1: User Activity Overview
            dcc.Tab(label='User Activity Overview', children=[
                html.Div([
                    # First row - two charts side by side
                    html.Div([
                        html.Div([
                            html.H3('Activity Distribution', style={'textAlign': 'center'}),
                            dcc.Graph(id='activity-distribution-pie')
                        ], style=styles['half_width']),

                        html.Div([
                            html.H3('User Activity Statistics', style={'textAlign': 'center'}),
                            dcc.Graph(id='user-activity-stats')
                        ], style=styles['half_width']),
                    ]),

                    # Second row - full width
                    html.Div([
                        html.Div([
                            html.H3('Top Users by Activity', style={'textAlign': 'center'}),
                            html.Div([
                                html.Label('Number of users to display:'),
                                dcc.Slider(
                                    id='top-users-slider',
                                    min=5,
                                    max=50,
                                    step=5,
                                    value=20,
                                    marks={i: str(i) for i in range(5, 51, 5)},
                                ),
                            ], style=styles['control_panel']),
                            dcc.Graph(id='top-users-chart')
                        ], style=styles['chart_container'])
                    ]),
                ])
            ]),

            # Tab 2: Trending Content Analysis
            dcc.Tab(label='Trending Content Analysis', children=[
                html.Div([
                    html.Div([
                        html.H3('Top Trending Content', style={'textAlign': 'center'}),
                        html.Div([
                            html.Label('Number of content items to display:'),
                            dcc.Slider(
                                id='top-content-slider',
                                min=5,
                                max=50,
                                step=5,
                                value=20,
                                marks={i: str(i) for i in range(5, 51, 5)},
                            ),
                        ], style=styles['control_panel']),
                        dcc.Graph(id='trending-content-chart')
                    ], style=styles['chart_container']),

                    html.Div([
                        html.H3('Engagement Distribution', style={'textAlign': 'center'}),
                        dcc.Graph(id='engagement-distribution')
                    ], style=styles['chart_container'])
                ])
            ]),

            # Tab 3: Activity Correlation
            dcc.Tab(label='Activity Correlation', children=[
                html.Div([
                    html.Div([
                        html.H3('Correlation Between Activity Types', style={'textAlign': 'center'}),
                        dcc.Graph(id='activity-correlation-heatmap')
                    ], style=styles['chart_container']),

                    html.Div([
                        html.H3('Scatter Plot Analysis', style={'textAlign': 'center'}),
                        html.Div([
                            html.Div([
                                html.Label('X-Axis:'),
                                dcc.Dropdown(
                                    id='x-axis-dropdown',
                                    options=[
                                        {'label': 'Posts', 'value': 'posts'},
                                        {'label': 'Likes', 'value': 'likes'},
                                        {'label': 'Comments', 'value': 'comments'},
                                        {'label': 'Shares', 'value': 'shares'}
                                    ],
                                    value='posts'
                                )
                            ], style={'width': '30%', 'display': 'inline-block'}),

                            html.Div([
                                html.Label('Y-Axis:'),
                                dcc.Dropdown(
                                    id='y-axis-dropdown',
                                    options=[
                                        {'label': 'Posts', 'value': 'posts'},
                                        {'label': 'Likes', 'value': 'likes'},
                                        {'label': 'Comments', 'value': 'comments'},
                                        {'label': 'Shares', 'value': 'shares'}
                                    ],
                                    value='likes'
                                )
                            ], style={'width': '30%', 'display': 'inline-block', 'marginLeft': '5%'}),
                        ], style=styles['control_panel']),
                        dcc.Graph(id='activity-scatter-plot')
                    ], style=styles['chart_container'])
                ])
            ])
        ], style=styles['tab_container'])
    ]),

    # Footer
    html.Div([
        html.Hr(),
        html.P('Social Media Analytics MapReduce Workflow - Dashboard'),
        html.P('Created with Dash and Plotly')
    ], style=styles['footer'])
], style=styles['container'])


# Define callbacks for interactive elements
@app.callback(
    Output('activity-distribution-pie', 'figure'),
    Input('activity-distribution-pie', 'id')  # Dummy input to trigger the callback
)
def update_activity_pie(dummy):
    if not has_user_data:
        return go.Figure().update_layout(title_text="No user activity data available")

    # Calculate totals for each activity type
    totals = {
        'Posts': user_activity_df['posts'].sum(),
        'Likes': user_activity_df['likes'].sum(),
        'Comments': user_activity_df['comments'].sum(),
        'Shares': user_activity_df['shares'].sum()
    }

    # Create pie chart
    fig = go.Figure(data=[go.Pie(
        labels=list(totals.keys()),
        values=list(totals.values()),
        hole=.3,
        marker=dict(colors=px.colors.qualitative.Set3)
    )])

    fig.update_layout(
        title_text='Distribution of Social Media Activities',
        showlegend=True
    )

    return fig


@app.callback(
    Output('user-activity-stats', 'figure'),
    Input('user-activity-stats', 'id')  # Dummy input to trigger the callback
)
def update_activity_stats(dummy):
    if not has_user_data:
        return go.Figure().update_layout(title_text="No user activity data available")

    # Calculate statistics for each activity type
    stats = {
        'Average': [
            user_activity_df['posts'].mean(),
            user_activity_df['likes'].mean(),
            user_activity_df['comments'].mean(),
            user_activity_df['shares'].mean()
        ],
        'Median': [
            user_activity_df['posts'].median(),
            user_activity_df['likes'].median(),
            user_activity_df['comments'].median(),
            user_activity_df['shares'].median()
        ],
        'Max': [
            user_activity_df['posts'].max(),
            user_activity_df['likes'].max(),
            user_activity_df['comments'].max(),
            user_activity_df['shares'].max()
        ]
    }

    activity_types = ['Posts', 'Likes', 'Comments', 'Shares']

    # Create grouped bar chart
    fig = go.Figure()

    for stat_name, stat_values in stats.items():
        fig.add_trace(go.Bar(
            name=stat_name,
            x=activity_types,
            y=stat_values
        ))

    fig.update_layout(
        barmode='group',
        title_text='User Activity Statistics',
        xaxis_title='Activity Type',
        yaxis_title='Count'
    )

    return fig


@app.callback(
    Output('top-users-chart', 'figure'),
    Input('top-users-slider', 'value')
)
def update_top_users(top_n):
    if not has_user_data:
        return go.Figure().update_layout(title_text="No user activity data available")

    # Get top N users by total activity
    top_users = user_activity_df.nlargest(top_n, 'total_activity')

    # Create stacked bar chart
    fig = go.Figure()

    # Add each activity type as a separate trace
    for activity, color in zip(
            ['posts', 'comments', 'shares', 'likes'],
            px.colors.qualitative.Set1
    ):
        fig.add_trace(go.Bar(
            name=activity.capitalize(),
            x=top_users['user_id'],
            y=top_users[activity],
            marker_color=color
        ))

    fig.update_layout(
        title_text=f'Top {top_n} Most Active Users',
        xaxis_title='User ID',
        yaxis_title='Activity Count',
        barmode='stack',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        height=600
    )

    # Rotate x-axis labels if there are many users
    if top_n > 10:
        fig.update_layout(
            xaxis=dict(
                tickangle=-45,
                tickfont=dict(size=10)
            )
        )

    return fig


@app.callback(
    Output('trending-content-chart', 'figure'),
    Input('top-content-slider', 'value')
)
def update_trending_content(top_n):
    if not has_content_data:
        return go.Figure().update_layout(title_text="No trending content data available")

    # Get top N content items by engagement
    top_content = trending_content_df.nlargest(top_n, 'engagement')

    # Create horizontal bar chart
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=top_content['engagement'],
        y=top_content['content_id'],
        orientation='h',
        marker=dict(
            color=top_content['engagement'],
            colorscale='Viridis'
        )
    ))

    fig.update_layout(
        title_text=f'Top {top_n} Trending Content Items',
        xaxis_title='Engagement Score (Likes + Shares)',
        yaxis_title='Content ID',
        height=600
    )

    return fig


@app.callback(
    Output('engagement-distribution', 'figure'),
    Input('engagement-distribution', 'id')  # Dummy input to trigger the callback
)
def update_engagement_distribution(dummy):
    if not has_content_data:
        return go.Figure().update_layout(title_text="No trending content data available")

    # Create histogram
    fig = go.Figure()

    fig.add_trace(go.Histogram(
        x=trending_content_df['engagement'],
        nbinsx=50,
        marker_color='teal'
    ))

    # Add KDE curve
    # Calculate KDE
    kde_x = np.linspace(
        trending_content_df['engagement'].min(),
        trending_content_df['engagement'].max(),
        1000
    )
    kde = stats.gaussian_kde(trending_content_df['engagement'])
    kde_y = kde(kde_x) * trending_content_df['engagement'].count()

    fig.add_trace(go.Scatter(
        x=kde_x,
        y=kde_y,
        mode='lines',
        line=dict(color='red', width=2),
        name='KDE'
    ))

    # Add mean and median lines
    mean_engagement = trending_content_df['engagement'].mean()
    median_engagement = trending_content_df['engagement'].median()

    fig.add_vline(
        x=mean_engagement,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Mean: {mean_engagement:.1f}",
        annotation_position="top"
    )

    fig.add_vline(
        x=median_engagement,
        line_dash="dot",
        line_color="green",
        annotation_text=f"Median: {median_engagement:.1f}",
        annotation_position="bottom"
    )

    fig.update_layout(
        title_text='Distribution of Content Engagement Scores',
        xaxis_title='Engagement Score',
        yaxis_title='Frequency'
    )

    return fig


@app.callback(
    Output('activity-correlation-heatmap', 'figure'),
    Input('activity-correlation-heatmap', 'id')  # Dummy input to trigger the callback
)
def update_correlation_heatmap(dummy):
    if not has_user_data:
        return go.Figure().update_layout(title_text="No user activity data available")

    # Calculate correlation matrix
    correlation_matrix = user_activity_df[['posts', 'likes', 'comments', 'shares']].corr()

    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=correlation_matrix.values,
        x=['Posts', 'Likes', 'Comments', 'Shares'],
        y=['Posts', 'Likes', 'Comments', 'Shares'],
        colorscale='RdBu_r',
        zmin=-1,
        zmax=1,
        text=correlation_matrix.round(2).values,
        texttemplate="%{text}",
        textfont={"size": 12}
    ))

    fig.update_layout(
        title_text='Correlation Between Different Activity Types',
        height=500
    )

    return fig


@app.callback(
    Output('activity-scatter-plot', 'figure'),
    [Input('x-axis-dropdown', 'value'),
     Input('y-axis-dropdown', 'value')]
)
def update_scatter_plot(x_activity, y_activity):
    if not has_user_data:
        return go.Figure().update_layout(title_text="No user activity data available")

    # Create scatter plot
    fig = px.scatter(
        user_activity_df,
        x=x_activity,
        y=y_activity,
        color='total_activity',
        color_continuous_scale='Viridis',
        hover_data=['user_id', 'posts', 'likes', 'comments', 'shares'],
        opacity=0.7
    )

    # Add trend line
    fig.update_traces(marker=dict(size=10, line=dict(width=2, color='DarkSlateGrey')))

    fig.update_layout(
        title_text=f'Relationship Between {x_activity.capitalize()} and {y_activity.capitalize()}',
        xaxis_title=x_activity.capitalize(),
        yaxis_title=y_activity.capitalize(),
        height=500
    )

    # Add trend line
    fig.update_layout(
        shapes=[
            dict(
                type='line',
                x0=user_activity_df[x_activity].min(),
                y0=user_activity_df[y_activity].min(),
                x1=user_activity_df[x_activity].max(),
                y1=user_activity_df[y_activity].max(),
                line=dict(color='Red', dash='dash')
            )
        ]
    )

    return fig


# Run the server
if __name__ == '__main__':
    print("Starting Social Media Analytics Dashboard...")
    print(f"Loaded {len(user_activity_df)} user activity records")
    print(f"Loaded {len(trending_content_df)} trending content records")
    print("Open your web browser and navigate to http://127.0.0.1:8050/")
    app.run(debug=True)