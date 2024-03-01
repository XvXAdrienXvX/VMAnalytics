# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.
from dash import Dash, html, dcc
import plotly.express as px
import pandas as pd
import plotly.graph_objs as go
app = Dash(__name__)
server = app.server

mean_df = pd.read_csv('Datasets/mean_df.csv')
mean_df_base_score_attack_vector = pd.read_csv('Datasets/mean_df_base_score_attack_vector.csv')
mode_df_base_score_attack_vector = pd.read_csv('Datasets/mode_df_base_score_attack_vector.csv')
base_severity_count = pd.read_csv('Datasets/base_severity_count_attack_vector.csv')
base_metrics_impact_score_correlation = pd.read_csv('Datasets/base_metrics_impact_score_correlation.csv')
correlation_impact_score = pd.read_csv('Datasets/correlation.csv')

mean = px.line(mean_df, x="Year", y="Base Score", width=600, height=500, title="Yearly Average Base Score")

mean_trace = go.Bar(
    x=mean_df_base_score_attack_vector["Attack Vector"],
    y=mean_df_base_score_attack_vector["Base Score"],
    name="Mean"
)


mode_trace = go.Bar(
    x=mode_df_base_score_attack_vector["Attack Vector"],
    y=mode_df_base_score_attack_vector["Base Score"],
    name="Mode"
)


layout = go.Layout(
    title="Distribution of Mean and Mode by Attack Vector",
    xaxis_title="Attack Vector",
    yaxis_title="Base Score",
    barmode="group" 
)

# Create the figure and add the traces
go_bar_chart_base_score = go.Figure(data=[mean_trace, mode_trace], layout=layout)

severity_order = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
custom_colors = {
    "LOW": "green",
    "MEDIUM": "darkorange",
    "HIGH": "red",
    "CRITICAL": "purple"
}
options = {
    'xAxisLabel': "<span style='letter-spacing: 1.3px;'>Attack Vector</span>",
    'yAxisLabel': "<span style='letter-spacing: 1.3px;'>Base Severity Count</span>",
    'width': 600,
    'height': 1000,
    'title': "Base Severity Count by Attack Vector",
    'format': "",
    'font_size': 16,
    'font_color': "white",
    'value_color': "white",
    'value_size': 12
}
stacked_bar_chart_severity = px.bar(
base_severity_count,
x="attackVector",
y="count",
color="baseSeverity",
category_orders={"baseSeverity": severity_order},
labels={"attackVector": options['xAxisLabel'], "count": options['yAxisLabel'], "baseSeverity": options['yAxisLabel']},
title=options['title'],
color_discrete_map=custom_colors
)
label_font = dict(size=options['font_size'], color=options['font_color'])
tick_font = dict(size=options['value_size'], color=options['value_color'])
stacked_bar_chart_severity.update_xaxes(title_text=options['xAxisLabel'], title_font=label_font, tickfont = tick_font)
stacked_bar_chart_severity.update_yaxes(title_text=options['yAxisLabel'], title_font=label_font, tickfont = tick_font)
stacked_bar_chart_severity.update_layout(barmode='stack', width=options['width'], height=options['height'])

bubble_chart_metrics_options = {
    'xAxisLabel': "<span style='letter-spacing: 1.3px;'>Exploitability Score</span>",
    'yAxisLabel': "<span style='letter-spacing: 1.3px;'>Base Score</span>",
    'width': 600,
    'height': 500,
    'title': "Base Metrics Data and Impact Score Strength",
    'format': "",
    'font_size': 16,
    'font_color': "black",
    'value_color': "black",
    'value_size': 12,
     'size_col': "impactScore",
     'color_col': "attackVector",
     'key_label': "Attack Vector"
}

          
base_metrics_bubble_chart = px.scatter(
    base_metrics_impact_score_correlation,
    x=  "exploitabilityScore",
    y= "baseScore",
    size= "impactScore",
    color= "Attack Vector",
    labels={"exploitabilityScore": bubble_chart_metrics_options['xAxisLabel'], "baseScore": bubble_chart_metrics_options['yAxisLabel']},
    title=bubble_chart_metrics_options['title'],
    width=bubble_chart_metrics_options['width'],
    height=bubble_chart_metrics_options['height']
)        
         
heat_map_impact_score = px.imshow(correlation_impact_score, zmin=-1, zmax=1, color_continuous_scale=px.colors.sequential.Cividis_r)

font_style = dict(size=options['value_size'], color='white')

for i in range(len(correlation_impact_score.index)):
    for j in range(len(correlation_impact_score.columns)):
        heat_map_impact_score.add_annotation(text=str(round(correlation_impact_score.iloc[i, j], 2)),
                        x=correlation_impact_score.columns[j],
                        y=correlation_impact_score.index[i],
                        showarrow=False,
                        font=font_style)

correlation_options = {
    'width': 600,
    'height': 500,
    'title': "Correlation Matrix: Impact Score Sub-Metrics",
    'format': "",
    'font_size': 16,
    'font_color': "white",
    'value_color': "white",
    'value_size': 13,
}

heat_map_impact_score.update_layout(
    xaxis=dict(title_font=dict(size=correlation_options['font_size'], color=correlation_options['font_color']),
            tickfont=dict(size=correlation_options['value_size'], color=correlation_options['value_color'])),
    yaxis=dict(title_font=dict(size=correlation_options['font_size'], color=correlation_options['font_color']),
            tickfont=dict(size=correlation_options['value_size'], color=correlation_options['value_color'])),
    width=correlation_options['width'], height=correlation_options['height'], title=correlation_options['title']
)         

mean.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font=dict(color='white')
)

go_bar_chart_base_score.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font=dict(color='white'),
    height = 500,
    width = 600
)

stacked_bar_chart_severity.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font=dict(color='white')
)

base_metrics_bubble_chart.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font=dict(color='white')
)

heat_map_impact_score.update_layout(
    plot_bgcolor='black',
    paper_bgcolor='black',
    font=dict(color='white')
)
                 
app.layout = html.Div(children=[
    html.Div(
        children=[
            dcc.Graph(
                id='graph3',
                figure=stacked_bar_chart_severity
            )
        ],
        style={'flex': '1', 'margin-left': '10px', 'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}  # Left column style
    ),
    html.Div(
        children=[
            html.Div(
                children=[
                    dcc.Graph(
                        id='graph1',
                        figure=mean
                    ),
                      dcc.Graph(
                        id='graph4',
                        figure=base_metrics_bubble_chart
                    )
                ],
                style={'flex': '1', 'margin-bottom': '3px', 'display': 'flex', 'flex-direction': 'row', 'background-color': '#111', 'gap': '10px'}  # Graph1 style
            ),
            html.Div(
                children=[
                    dcc.Graph(
                        id='graph2',
                        figure=go_bar_chart_base_score
                    ),
                    dcc.Graph(
                        id='graph5',
                        figure=heat_map_impact_score
                    )
                ],
                style={'flex': '1', 'display': 'flex', 'flex-direction': 'row', 'background-color': '#111', 'gap': '10px'}  # Graph2 and Graph5 style
            )
        ],
        style={'flex': '1', 'display': 'flex', 'flex-direction': 'column', 'gap': '10px'}  # Right column upper row layout
    ),
   
],
style={'display': 'flex', 'flex-wrap': 'wrap', 'background-color': '#111', 'justify-content': 'center'})  # Overall layout with black background



# if __name__ == '__main__':
#     app.run(debug=True)
if __name__ == '__main__':
    app.run_server(debug=False)