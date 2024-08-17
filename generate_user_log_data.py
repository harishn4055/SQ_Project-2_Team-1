import random
import time
import json
from datetime import datetime, timedelta

def generate_log_entry(year, month):
    # Generate a random date within the specified month and year
    start_date = datetime(year, month, 1)
    
    # Get the number of days in the month (handling edge cases like February)
    if month == 12:
        next_month_start = datetime(year + 1, 1, 1)
    else:
        next_month_start = datetime(year, month + 1, 1)
    
    days_in_month = (next_month_start - start_date).days
    random_days = random.randint(0, days_in_month - 1)
    random_seconds = random.randint(0, 86400 - 1)  # number of seconds in a day
    timestamp_date = start_date + timedelta(days=random_days, seconds=random_seconds)

    log_entry = {
        "timestamp": int(timestamp_date.timestamp()),
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

def create_log_file_for_year_range(start_year, end_year):
    for year in range(start_year, end_year + 1):  # Loop through each year
        filename = f"log_data_{year}.json"
        with open(filename, "w") as file:
            for month in range(1, 13):  # Loop through all months from January to December
                log_data = [generate_log_entry(year, month) for _ in range(100)]  # 100 entries per month
                for entry in log_data:
                    file.write(entry + "\n")
        print(f"Log data for {year} (all months) generated successfully in '{filename}'.")

# Generate log data for the years 1999 to 2002
create_log_file_for_year_range(1999, 2002)
