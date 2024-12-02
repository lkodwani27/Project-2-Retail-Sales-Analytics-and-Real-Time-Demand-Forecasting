import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import sqlite3

# Initialize Dash App
app = dash.Dash(__name__)

# Layout for the dashboard
app.layout = html.Div([
    html.H1("Real-Time Retail Dashboard"),
    
    html.Div([
        html.H3("Running Sales"),
        dcc.Graph(id="running-sales-graph")
    ]),
    
    html.Div([
        html.H3("Anomalies"),
        dcc.Graph(id="anomalies-graph")
    ]),
    
    dcc.Interval(id="update-interval", interval=2000, n_intervals=0)
])

# Callback to update running sales
@app.callback(
    Output("running-sales-graph", "figure"),
    [Input("update-interval", "n_intervals")]
)
def update_running_sales(n):
    conn = sqlite3.connect("/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db")
    query = "SELECT * FROM running_sales"
    df = pd.read_sql(query, conn)
    conn.close()

    fig = px.bar(df, x="Product", y=["sum(Quantity)", "sum(Price)"], barmode="group",
                 title="Running Sales per Product")
    return fig

# Callback to update anomalies
@app.callback(
    Output("anomalies-graph", "figure"),
    [Input("update-interval", "n_intervals")]
)
def update_anomalies(n):
    conn = sqlite3.connect("/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db")
    query = "SELECT * FROM anomalies"
    df = pd.read_sql(query, conn)
    conn.close()

    fig = px.scatter(df, x="Timestamp", y="Price", color="Product", size="Quantity",
                     title="Anomalies Detected")
    return fig

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
