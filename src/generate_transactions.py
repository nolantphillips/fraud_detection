import socket
import json
import random
import uuid
from datetime import datetime, timedelta
import threading
import time

# Config

HOST = "localhost"
PORT = 9999
USERS = [f"user_{i}" for i in range (1, 10001)]
MERCHANTS = ["grocery", "electronics", "clothing", "restaurants", "travel", "subscriptions"]
FRAUD_RATE = 0.01
TRANSACTIONS_PER_SECOND = 500

# Transaction Generator

def generate_transaction():
    user_id = random.choice(USERS)
    merchant = random.choice(MERCHANTS)

    if merchant == "grocery":
        amount = round(random.uniform(5, 100), 2)
    elif merchant == "electronics":
        amount = round(random.uniform(50, 1000), 2)
    elif merchant == "clothing":
        amount = round(random.uniform(10, 300), 2)
    elif merchant == "restaurants":
        amount = round(random.uniform(5, 150), 2)
    elif merchant == "travel":
        amount = round(random.uniform(100,2000), 2)
    else:
        amount = round(random.uniform(1, 50), 2)

    is_fraud = 1 if random.random() < FRAUD_RATE else 0

    if is_fraud:
        amount *= random.uniform(1.5, 3)

    txn = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "merchant": merchant,
        "amount": amount,
        "timestamp": datetime.now().isoformat(),
        "is_fraud": is_fraud
    }

    return json.dumps(txn) + "\n"

def handle_client(conn, addr):
    print(f"[INFO] CLient connected: {addr}")
    try:
        while True:
            for _ in range(TRANSACTIONS_PER_SECOND):
                txn = generate_transaction()
                conn.sendall(txn.encode("utf-8"))
            time.sleep(1)
    except (ConnectionResetError, BrokenPipeError):
        print(f"[INFO] Client disconnected: {addr}")
    finally:
        conn.close()

# Start socket server

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((HOST, PORT))
s.listen()

print(f"[INFO] Server listening on {HOST}:{PORT}...")

while True:
    conn, addr = s.accept()
    client_thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
    client_thread.start()