import random
import time
import json

def generate_log_entry():
    log_entry = {
        "timestamp": int(time.time()),
        "log_level": random.choice(["INFO", "WARN", "ERROR"]),
        "event_type": random.choice(["Authentication Failure", "Unauthorized Access", "Suspicious IP", "User Login", "User Logout"]),
        "message": random.choice([
            "Failed login attempt",
            "User attempted to access restricted file",
            "Login attempt from blacklisted IP",
            "User login",
            "User logout"
        ]),
        "user_id": random.randint(1000, 9999),
        "source_ip": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
        "destination_ip": f"10.0.{random.randint(0, 10)}.{random.randint(1, 255)}",
        "location": random.choice(["New York, USA", "London, UK", "Moscow, Russia", "Berlin, Germany", "Tokyo, Japan"]),
        "user_agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Linux; Android 9; SM-J730G Build/PPR1.180610.011)",
            "curl/7.68.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"
        ]),
        "action_taken": random.choice([
            "Account locked after 3 failed attempts",
            "Access denied",
            "IP blocked",
            "No action needed"
        ])
    }
    return json.dumps(log_entry)

log_data = [generate_log_entry() for _ in range(100)]

with open("log_data_2.json", "w") as file:
    for entry in log_data:
        file.write(entry + "\n")

print("Enhanced log data generated successfully.")
