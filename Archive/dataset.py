import random
import time
import json

def generate_log_entry():
    log_entry = {
        "timestamp": int(time.time()),
        "log_level": random.choice(["INFO", "WARN", "ERROR"]),
        "message": random.choice(["User login", "User logout", "File uploaded", "File deleted", "System error"]),
        "user_id": random.randint(1000, 9999),
    }
    return json.dumps(log_entry)

log_data = [generate_log_entry() for _ in range(150)]

with open("log_data.json", "w") as file:
    for entry in log_data:
        file.write(entry + "\n")
