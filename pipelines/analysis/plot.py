"""
~-- PLOT
Prepares the raw data, making it ready for analysis. 
Stores dataset in data/2_final
"""
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import chart_studio.plotly as py

from datetime import datetime
import logging


def set_layout_template():
    
    # watermarks
    watermark_date = "Updated {}".format(datetime.now().strftime("%d.%B%Y") )# date watermark
    watermark_url = "towardssustainablefinance.com"
    
    # colorscale
    tsf_colorscale = ["#4d886d", "#f3dab9", "#9bcab8", "#829fa5", "#dc9b4d", "#4a82a1", "#cfaea5", '#D5E6E0']
    

    pio.templates["tsf"] = go.layout.Template(
        layout_colorway=tsf_colorscale, 
        layout_hovermode= 'closest', 
        layout_font_family='Verdana',
        layout_annotations=[
            dict(
                name="watermark",
                text=watermark_url,
                textangle=0,
                opacity=0.65,
                font=dict(color="#545454", size=20),
                xref="paper",
                yref="paper",
                x=1,
                y=-0.15,
                showarrow=False,
            ), 
            dict(
                name="watermark2",
                text=watermark_date,
                textangle=0,
                opacity=0.65,
                font=dict(color="#545454", size=16),
                xref="paper",
                yref="paper",
                x=0,
                y=0.1,
                showarrow=False,
            )
        ]
    )
    pio.templates.default = 'tsf'


def plot_timeline(df):
    """line chart: daily aboslute change """
    # round to million
    df.results_count = (df.results_count/1000000)

    fig = px.line(df, x="query_timestamp", y="results_count", color="keyword", 
                  # title='Google results over time', 
                 labels={
                         "query_timestamp": "",
                         "results_count": "Google results",
                         "keyword": ""
                     },
                  text = df.keyword
                 )
    fig.update_traces(mode="markers+lines", hovertemplate="%{text}<br>" + "%{y:20.0f}Mio.<br>%{x}<extra></extra>") 
    
    # -- customize legend
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1,
        xanchor="right", 
        x=1, 
        bgcolor='rgba(0,0,0,0)'), # transparent  
        hovermode="closest",
    )
    return fig

def plot_change(df):
    """Last 30 days: Absolute and relative change"""
    
    # keyword label with line break 
    x_axis_labels = [s.replace(' ', '<br>') for s in df.keyword.unique()]

    # -- subplot grid
    fig = make_subplots(rows=1, cols=2, 
                        shared_yaxes=True,
                        shared_xaxes=False,
                        subplot_titles=['In numbers', 'In %'])
    
    # -- bar chart I: abosulte change
    fig.add_trace(
        go.Bar(
                x=df.lxdays_absolute_change,
                y=x_axis_labels,
                orientation='h', 
                # <extra></extra> removes trace info
                hovertemplate="<b>%{y}</b><br><br>" + "%{x:20s}<br><extra></extra>",
        ), 
        row=1, col=1
    )
    fig.update_yaxes(ticklabelposition='outside', side='right')

    # -- bar chart II: relative change
    fig.add_trace(
        go.Bar(
                x=df.lxdays_relative_change,
                y=x_axis_labels,
                orientation='h',
                # <extra></extra> removes trace info
                hovertemplate="<b>%{y}</b><br><br>" + "%{x:0f}%<br><extra></extra>",
        ), 
        row=1, col=2
    )

    # colorscale
    tsf_colorscale = ["#4d886d", "#f3dab9", "#9bcab8", "#829fa5", "#dc9b4d", "#4a82a1", "#cfaea5", '#D5E6E0']
    fig.update_traces(marker=dict(color=tsf_colorscale), 
                    showlegend=False)

    # -- finalize: size, title and hover text 
    last_x_days = (df.query_timestamp.max()-df.date_lxdays.min()).days
    fig.update_layout(title_text=f"How did the amount of Google results change over the last {last_x_days} days?",
                    hoverlabel=dict(
                    bgcolor="white",
                    font_size=14,
                    ),
                    # width=1000, height=550
                    )
    return fig


def deploy_figure(figure, filename):    
    """ Upload graph to chartstudio """
    logging.info(f"Upload {filename} figure to plotly")
    py.plot(figure, filename=filename)



# ------------------ TESTING ------------------ 
# import streamlit as st
# import sys
# sys.path.append("../")
# from transform import impute_results_count, feature_engineering, subset_last_x_days
# from load_raw_data import load
# from transform import subset_last_x_days

# df = (load(raw_data_dir='../../data/0_raw', filename='google_results_count')
#       .pipe(impute_results_count)
#      .pipe(feature_engineering))

# # -- Last x days
# import pandas as pd
# df_lxdays = subset_last_x_days(df, last_x_days=30)


# set_layout_template()
# fig_timeline = plot_timeline(df)
# fig_change = plot_change(df_lxdays)

# st.plotly_chart(fig_timeline)

# deploy_figure(fig_timeline, filename="google_results_count_timeline")
# deploy_figure(fig_change, filename="google_results_count_change")
