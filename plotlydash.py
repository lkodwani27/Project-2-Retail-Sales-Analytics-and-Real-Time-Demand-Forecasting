from flask import Flask
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd

# Flask app
server = Flask(__name__)

# Dash app
app = dash.Dash(__name__, server=server)

# Placeholder for real-time data
data = pd.DataFrame(columns=["Product ID", "Total Sales", "Transaction Count"])

app.layout = html.Div([
    html.H1("Real-Time Transaction Monitoring Dashboard"),
    dcc.Interval(id='update-interval', interval=2000, n_intervals=0),  # Update every 2 seconds
    html.Div(id='sales-data'),
    html.Div(id='anomalies-data')
])

@app.callback(
    Output('sales-data', 'children'),
    [Input('update-interval', 'n_intervals')]
)
def update_sales(n):
    # Mock: Replace with actual data source (e.g., database or file)
    global data
    table = data.to_html(index=False)
    return html.Div([
        html.H3("Sales Summary"),
        html.Div(table, style={'overflowX': 'scroll'})
    ])

@app.callback(
    Output('anomalies-data', 'children'),
    [Input('update-interval', 'n_intervals')]
)
def update_anomalies(n):
    # Mock: Replace with actual data source
    anomalies = data[data["Anomaly"] == "Anomaly"]
    table = anomalies.to_html(index=False)
    return html.Div([
        html.H3("Anomalies Detected"),
        html.Div(table, style={'overflowX': 'scroll'})
    ])

if __name__ == "__main__":
    app.run_server(debug=True)