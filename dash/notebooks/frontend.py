print("running front end!")

import backend

# dash imports
from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px

# data imports
import pandas as pd

# dh imports
import deephaven.pandas as dhpd
import deephaven.agg as agg

CTX, historical = backend.get_tables()
#with CTX:
    #historical2 = historical.update("low = Coin == `ETH` ? low % 9 : low")

app = Dash(__name__)

with CTX:
    app.layout = html.Div([
        html.H1(children='Title of Dash App', style={'textAlign':'center'}),
        dcc.Dropdown(dhpd.to_pandas(historical.agg_by(agg.unique("Coin"), by="Coin")).Coin.unique(), 'BTC', id='dropdown-selection'),
        dcc.Graph(id='graph-content')
    ])

@callback(
    Output('graph-content', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_graph(value):
    with CTX:
        dff = dhpd.to_pandas(historical.where(f"Coin == `{value}`"))
    return px.line(dff, x='dateTime', y='low')

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)