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
import deephaven.time as dhtu
import deephaven.agg as agg
from deephaven import merge

# plotting imports
import plotly.express as px

### custom function for time-based ring table
from datetime import timedelta
from deephaven.table import Table
from typing import Optional

def relative_time_window(
        table: Table,
        ts_col: str,
        window: timedelta,
        offset: timedelta = timedelta(seconds=0),
        snap: Optional[timedelta] = None) -> Table:

    j_window = dhtu.to_j_duration(window)
    j_offset = dhtu.to_j_duration(offset)
    current_time = table.agg_by(agg.sorted_last(ts_col, ts_col))
    table = table.natural_join(current_time, on=None, joins=f"CurrentTime={ts_col}")

    if snap is not None:
        j_snap = dhtu.to_j_duration(snap)
        return table.\
            update(f"SnappedTime = lowerBin({ts_col}, j_snap.toNanos())").\
            where(f"CurrentTime - SnappedTime < j_snap.toNanos() + j_offset.toNanos() + j_window.toNanos() && \
                    CurrentTime - SnappedTime > j_snap.toNanos() + j_offset.toNanos()").\
            drop_columns(["CurrentTime", "SnappedTime"])

    return table.\
        where(f"CurrentTime - {ts_col} < j_offset.toNanos() + j_window.toNanos() && \
                CurrentTime - {ts_col} > j_offset.toNanos()").\
        drop_columns("CurrentTime")

CTX, historical, streaming = backend.get_tables()
data = merge([historical, streaming])

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
                        options=[ # dash does not seem to support values that must be evaluated in any way
                            {'label': '5 seconds', 'value': 5},
                            {'label': '15 seconds', 'value': 15},
                            {'label': '30 seconds', 'value': 30},
                            {'label': '1 minute', 'value': 60},
                            {'label': '5 minutes', 'value': 300},
                            {'label': '15 minutes', 'value': 900},
                            {'label': '30 minutes', 'value': 1800}
                        ],
                        value=30,
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
                            id='open-value-card',
                            style={'textAlign': 'center', 'fontSize': 30}),
                        html.P(
                            id='open-delta-card',
                            style={'textAlign': 'center', 'fontSize': 15, 'margin-top': '-18px'})
                    ], body=True),

                    dbc.Card([
                        html.H4(
                            'High',
                            style={'textAlign': 'center'}),
                        html.P(
                            id='high-value-card',
                            style={'textAlign': 'center', 'fontSize': 30}),
                        html.P(
                            id='high-delta-card',
                            style={'textAlign': 'center', 'fontSize': 15, 'margin-top': '-18px'})
                    ], body=True),

                    dbc.Card([
                        html.H4(
                            'Low',
                            style={'textAlign': 'center'}),
                        html.P(
                            id='low-value-card',
                            style={'textAlign': 'center', 'fontSize': 30}),
                        html.P(
                            id='low-delta-card',
                            style={'textAlign': 'center', 'fontSize': 15, 'margin-top': '-18px'})
                    ], body=True),

                    dbc.Card([
                        html.H4(
                            'Close',
                            style={'textAlign': 'center'}),
                        html.P(
                            id='close-value-card',
                            style={'textAlign': 'center', 'fontSize': 30}),
                        html.P(
                            id='close-delta-card',
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
    return px.line(dff, x='Timestamp', y='Price'). \
        add_vrect(x0="2021-09-22T16:15:00 ET", x1="2021-09-22T16:13:00 ET", fillcolor="gold", opacity=1)

@dash.callback(
    dash.Output('open-value-card', 'children'),
    dash.Output('open-delta-card', 'children'),
    dash.Output('high-value-card', 'children'),
    dash.Output('high-delta-card', 'children'),
    dash.Output('low-value-card', 'children'),
    dash.Output('low-delta-card', 'children'),
    dash.Output('close-value-card', 'children'),
    dash.Output('close-delta-card', 'children'),
    dash.Input('instrument-selection', 'value'),
    dash.Input('period-selection', 'value'),
    dash.Input('interval-component', 'n_intervals')
)
def update_ohlc(instrument, period, n_intervals):
    with CTX:
        current_ohlc = relative_time_window(table=data,
                                            ts_col="Timestamp",
                                            window=timedelta(seconds=period),
                                            snap=timedelta(seconds=5)). \
            where(f"Instrument == `{instrument}`"). \
            agg_by([
            agg.last("CurrentOpen=Price"),
            agg.max_("CurrentHigh=Price"),
            agg.min_("CurrentLow=Price"),
            agg.first("CurrentClose=Price")])
        previous_ohlc = relative_time_window(table=data,
                                             ts_col="Timestamp",
                                             window=timedelta(seconds=period),
                                             offset=timedelta(seconds=period),
                                             snap=timedelta(seconds=5)). \
            where(f"Instrument == `{instrument}`"). \
            agg_by([
            agg.last("PreviousOpen=Price"),
            agg.max_("PreviousHigh=Price"),
            agg.min_("PreviousLow=Price"),
            agg.first("PreviousClose=Price")])
        ohlc = current_ohlc.natural_join(previous_ohlc, on=None). \
            update(["OpenDelta = 100 * (CurrentOpen - PreviousOpen) / PreviousOpen",
                    "HighDelta = 100 * (CurrentHigh - PreviousHigh) / PreviousHigh",
                    "LowDelta = 100 * (CurrentLow - PreviousLow) / PreviousLow",
                    "CloseDelta = 100 * (CurrentClose - PreviousClose) / PreviousClose"]). \
            drop_columns(["PreviousOpen", "PreviousHigh", "PreviousLow", "PreviousClose"])

        vals = dhpd.to_pandas(ohlc).to_dict('records')[0]
    return '${:,.2f}'.format(vals['CurrentOpen']), '{:.4}%'.format(vals['OpenDelta']), \
            '${:,.2f}'.format(vals['CurrentHigh']), '{:.4}%'.format(vals['HighDelta']), \
            '${:,.2f}'.format(vals['CurrentLow']), '{:.4}%'.format(vals['LowDelta']), \
            '${:,.2f}'.format(vals['CurrentClose']), '{:.4}%'.format(vals['CloseDelta'])


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)