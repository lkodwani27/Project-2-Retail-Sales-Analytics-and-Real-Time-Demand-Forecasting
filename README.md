Task5 

install SQLite from extension 

sudo apt update
sudo apt install -y openjdk-11-jdk sqlite3


pip install pyspark

pip install dash pandas plotly sqlite-jdbc



#To generate the steam of data
python3 generate_retail.py


#To execute the spark job 
spark-submit --jars /workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/jars/sqlite-jdbc-3.47.1.0.jar task5streaming.py

# To execute the Dashboard
python dashboard_app.py


# To view database
sqlite3 retail_data.db

sqlite3 /workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db

#To Check if the database has valid tables:
Copy code
.tables

#To view data 
SELECT * FROM running_sales;  -- Displays data from a table
SELECT * FROM anomalies;

# To query the schema of any existing tables
SELECT name, sql FROM sqlite_master WHERE type='table';
