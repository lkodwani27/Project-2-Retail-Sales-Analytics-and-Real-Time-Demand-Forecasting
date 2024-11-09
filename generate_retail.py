import time
import random
import socket
from datetime import datetime

# List of sample companies
companies = ["AAPL", "MSFT", "GOOG", "AMZN", "NFLX", "TSLA", "META", "IBM", "ORCL", "INTC"]

# Function to generate random stock prices
def generate_stock_price():
    company = random.choice(companies)
    price = round(random.uniform(100, 1500), 2)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{timestamp},{company},{price}"

def start_streaming():
    # Create a socket server
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', 9999))
    server_socket.listen(1)
    server_socket.settimeout(1.0)  # Set timeout for 1 second to allow checking for KeyboardInterrupt

    print("Waiting for connection... (Press Ctrl+C to stop)")
    
    while True:
        connection, addr = None, None
        try:
            try:
                connection, addr = server_socket.accept()  # This will raise a timeout exception if no connection is made within 1 second
            except socket.timeout:
                continue  # If no connection is made, continue and allow checking for Ctrl+C

            print("Connection established. Streaming data...")

            # Continuously send data to the client
            while True:
                stock_data = generate_stock_price()
                connection.send((stock_data + "\n").encode('utf-8'))
                print(f"Sent: {stock_data}")
                time.sleep(1)

        except ConnectionResetError:
            print("Connection lost. Waiting for new connection...")
            if connection:
                connection.close()  # Close the broken connection
            time.sleep(2)  # Delay before waiting for a new connection

        except KeyboardInterrupt:
            print("\nStreaming stopped by user.")
            break

        finally:
            if connection:
                connection.close()

    server_socket.close()

if __name__ == "__main__":
    start_streaming()