from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from threading import Thread
import pandas as pd
from flask import Flask
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from dash.dash_table import DataTable

# Flask app (for Dash)
server = Flask(__name__)

# Dash app
app = dash.Dash(__name__, server=server)

# Global DataFrame to store results for dashboard
sales_data = pd.DataFrame(columns=["Product ID", "Total Sales", "Transaction Count"])
anomalies_data = pd.DataFrame(columns=["Timestamp", "Product ID", "Quantity", "Price", "Anomaly"])

# Placeholder to hold Spark processing updates
sales_updates = []
anomalies_updates = []


# --- Spark Streaming Function ---
def spark_streaming_job():
    global sales_data, anomalies_data

    # Create Spark Session
    spark = SparkSession.builder \
        .appName("Real-Time Transaction Monitoring") \
        .getOrCreate()

    # Define schema for transactions
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True)
    ])

    # Define anomaly detection function
    def detect_anomalies(quantity, price):
        if quantity > 10 or price > 100:  # Example anomaly thresholds
            return "Anomaly"
        else:
            return "Normal"

    # Register the UDF
    anomaly_udf = udf(detect_anomalies, StringType())

    # Stream data from socket
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    # Parse the input data
    transactions = lines.withColumn("value", split(lines["value"], ",")) \
        .selectExpr(
            "value[0] as timestamp",
            "value[1] as product_id",
            "CAST(value[2] AS INT) as quantity",
            "CAST(value[3] AS DOUBLE) as price"
        )

    # Calculate running total sales
    sales_summary = transactions.groupBy("product_id") \
        .agg(
            sum(col("price") * col("quantity")).alias("total_sales"),
            count("*").alias("transaction_count")
        )

    # Add anomaly detection
    transactions_with_anomalies = transactions.withColumn(
        "anomaly", anomaly_udf(col("quantity"), col("price"))
    )

    # Write to memory for dashboard updates
    sales_query = sales_summary.writeStream \
        .outputMode("update") \
        .format("memory") \
        .queryName("sales_summary") \
        .start()

    anomalies_query = transactions_with_anomalies.writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName("anomalies") \
        .start()

    # Continuously update the global DataFrames for Dash
    while True:
        # Query the in-memory tables
        sales_updates_df = spark.sql("SELECT * FROM sales_summary").toPandas()
        anomalies_updates_df = spark.sql("SELECT * FROM anomalies WHERE anomaly = 'Anomaly'").toPandas()

        # Update global DataFrames
        if not sales_updates_df.empty:
            sales_data = sales_updates_df
        if not anomalies_updates_df.empty:
            anomalies_data = anomalies_updates_df
# --- Dash Dashboard Function ---
def run_dashboard():
    global sales_data, anomalies_data

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
        global sales_data
        if not sales_data.empty:
            return html.Div([
                html.H3("Sales Summary"),
                DataTable(
                    columns=[
                        {"name": col, "id": col} for col in sales_data.columns
                    ],
                    data=sales_data.to_dict('records'),
                    style_table={'overflowX': 'scroll'},
                    style_cell={
                        'textAlign': 'left',
                        'padding': '5px',
                        'whiteSpace': 'normal',
                    },
                    style_header={
                        'backgroundColor': 'rgb(230, 230, 230)',
                        'fontWeight': 'bold'
                    }
                )
            ])
        return "No data available yet."


    @app.callback(
        Output('anomalies-data', 'children'),
        [Input('update-interval', 'n_intervals')]
    )
    def update_anomalies(n):
        global anomalies_data
        if not anomalies_data.empty:
            return html.Div([
                html.H3("Anomalies Detected"),
                DataTable(
                    columns=[
                        {"name": col, "id": col} for col in anomalies_data.columns
                    ],
                    data=anomalies_data.to_dict('records'),
                    style_table={'overflowX': 'scroll'},
                    style_cell={
                        'textAlign': 'left',
                        'padding': '5px',
                        'whiteSpace': 'normal',
                    },
                    style_header={
                        'backgroundColor': 'rgb(230, 230, 230)',
                        'fontWeight': 'bold'
                    }
                )
            ])
        return "No anomalies detected yet."

    # Run Dash app
    app.run_server(debug=False, port=8050)


# --- Main Script ---
if __name__ == "__main__":
    # Run Spark Streaming in a separate thread
    spark_thread = Thread(target=spark_streaming_job)
    spark_thread.daemon = True  # Ensure it exits when the main program does
    spark_thread.start()

    # Run the Dash Dashboard
    run_dashboard()
