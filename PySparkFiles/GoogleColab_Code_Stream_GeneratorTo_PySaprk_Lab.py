import json
import os
import random
import uuid
import time
import csv
from datetime import datetime

# Install sshpass for password-based authentication
!apt-get install -y sshpass

# Define remote server details
REMOTE_USER = "labuser"
REMOTE_HOST = "34.21.3.228"
REMOTE_PATH = "/home/labuser/Documents/Day4/StreamingData/"
REMOTE_PASSWORD = "Mkfastets@321"

# Configuration
TRANSACTION_TYPES = ["POS", "Online", "ATM Withdrawal", "Transfer"]
LOCATIONS = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
OUTPUT_DIR = "streaming_data_path"
BATCH_SIZE = 5  # Number of transactions per batch
SLEEP_INTERVAL = 2  # Time interval between batches (seconds)

# Ensure the output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Generate random transaction data
def generate_transaction_record():
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"customer_{random.randint(1, 500)}",
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "location": random.choice(LOCATIONS),
        "timestamp": datetime.now().isoformat()
    }

# Write transactions continuously
def generate_streaming_data():
    batch_number = 1
    while True:
        batch_data = [generate_transaction_record() for _ in range(BATCH_SIZE)]
        filename = os.path.join(OUTPUT_DIR, f"transactions_batch_{batch_number}.json")
        
        # Write batch to a JSON file
        with open(filename, 'w') as f:
            json.dump(batch_data, f, indent=2)

        print(f"Batch {batch_number} written with {BATCH_SIZE} transactions.")

        # Upload file to remote server
        os.system(f"sshpass -p '{REMOTE_PASSWORD}' scp {filename} {REMOTE_USER}@{REMOTE_HOST}:{REMOTE_PATH}")

        batch_number += 1

        # Wait before generating the next batch
        time.sleep(SLEEP_INTERVAL)

if __name__ == "__main__":
    print("Starting streaming data generation...")
    try:
        generate_streaming_data()
    except KeyboardInterrupt:
        print("Streaming data generation stopped.")
