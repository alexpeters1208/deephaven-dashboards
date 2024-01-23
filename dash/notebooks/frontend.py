import backend

# dash imports
import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc

# data imports
import pandas as pd

# dh imports
import deephaven.pandas as dhpd
import deephaven.agg as agg

# plotting imports
import plotly.express as px

### custom function for time-based ring table
from datetime import timedelta
from deephaven.table import Table

def relative_time_window(table: Table, ts_col: str, window: timedelta) -> Table:
    total_nanoseconds = (int)(window / timedelta(microseconds=1)) * 1000
    return table.\
        natural_join(
            streaming.head(1).select(ts_col), on=None, joins=f"CurrentTimestamp={ts_col}").\
        update(f"InWindow = diffNanos({ts_col}, CurrentTimestamp) < {total_nanoseconds}").\
        where("InWindow").\
        drop_columns(["CurrentTimestamp", "InWindow"])

CTX, historical, streaming = backend.get_tables()

with CTX:
    pd_historical = dhpd.to_pandas(historical)

app = dash.Dash(external_stylesheets=[dbc.themes.SPACELAB])

with CTX:
    app.layout = dbc.Container([
        dcc.Interval(
            id='interval-component',
            interval=1 * 1000,  # in milliseconds
            n_intervals=0),

        dbc.Row([
            html.H1(
                'Live Cryptocurrency Analysis',
                style={'textAlign': 'center'}),
            html.Hr(className="my-2")
        ]),

        dbc.Row([
            dbc.Col([
                dbc.Card([
                    html.H3(
                        'Instrument',
                        style={'textAlign': 'center'}),
                    dcc.Dropdown(
                        options=dhpd.to_pandas(
                            historical.agg_by(agg.unique("Instrument"), by="Instrument")).Instrument.unique(),
                        value='BTC/USD',
                        id='instrument-selection'),
                    html.Hr(className="my-2"),
                    html.H3(
                        'Period',
                        style={'textAlign': 'center'}),
                    dcc.Dropdown(
                        options=['30 seconds', '1 minute', '5 minutes', '15 minutes', '1 hour', '3 hours', '12 hours',
                                 '24 hours'],
                        value='30 seconds',
                        id='period-selection')
                ], body=True),
            ], width=3),

            dbc.Col([
                dcc.Graph(
                    id='graph-content',
                    style={'display': 'inline-block', 'columnWidth': '74vw'})
            ], width=7),

            dbc.Col([
                dbc.Stack([
                    dbc.Card([
                        html.H4(
                            'Open',
                            style={'textAlign': 'center'}),
                        html.P(
                            streaming.where(""),
                            style={'textAlign': 'center', 'fontSize': 30}),
                        html.P(
                            '-1%',
                            style={'textAlign': 'center', 'fontSize': 15, 'margin-top': '-18px'})
                    ], body=True),

                    dbc.Card([
                        html.H4(
                            'High',
                            style={'textAlign': 'center'}),
                        html.P(
                            "46,000.45",
                            style={'textAlign': 'center', 'fontSize': 30}),
                        html.P(
                            '-1%',
                            style={'textAlign': 'center', 'fontSize': 15, 'margin-top': '-18px'})
                    ], body=True),

                    dbc.Card([
                        html.H4(
                            'Low',
                            style={'textAlign': 'center'}),
                        html.P(
                            "46,000.45",
                            style={'textAlign': 'center', 'fontSize': 30}),
                        html.P(
                            '-1%',
                            style={'textAlign': 'center', 'fontSize': 15, 'margin-top': '-18px'})
                    ], body=True),

                    dbc.Card([
                        html.H4(
                            'Close',
                            style={'textAlign': 'center'}),
                        html.P(
                            "46,000.45",
                            style={'textAlign': 'center', 'fontSize': 30}),
                        html.P(
                            '-1%',
                            style={'textAlign': 'center', 'fontSize': 15, 'margin-top': '-18px'})
                    ], body=True)
                ], gap=3)
            ], width=2)
        ]),
    ], fluid=True)

@dash.callback(
    dash.Output('graph-content', 'figure'),
    dash.Input('instrument-selection', 'value'),
    dash.Input('interval-component', 'n_intervals')
)
def update_graph(instrument, n_intervals):
    with CTX:
        dff = dhpd.to_pandas(streaming.where(f"Instrument == `{instrument}`"))
    return px.line(dff, x='Timestamp', y='Price')

def update_olhc(instrument, n_intervals):
    with CTX:
        streaming.where([f"Instrument == `{instrument}`"])

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)