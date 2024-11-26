import time
import random
import socket
from datetime import datetime

# Generate sample Product IDs
products = [f"P{i:03}" for i in range(1, 301)]  # P001, P002, ..., P300

# Function to generate random stock prices
def generate_transaction():
    product_id = random.choice(products)
    quantity = random.randint(1, 80)  # Random quantity between 1 and 80
    price = round(random.uniform(1, 1000), 2)  # Random price between $1 and $1000
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"{timestamp},{product_id},{quantity},{price}"

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
                transaction_data = generate_transaction()
                connection.send((transaction_data + "\n").encode('utf-8'))
                print(f"Sent: {transaction_data}")
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