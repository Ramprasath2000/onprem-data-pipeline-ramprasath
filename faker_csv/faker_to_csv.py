import time
import csv
from faker import Faker
from datetime import datetime

fake = Faker()
CSV_FILE = "/data/fake_weather_logs.csv"

def fake_log():
    return {
        "name": fake.name(),
        "city": fake.city(),
        "temperature": round(fake.pyfloat(min_value=-10, max_value=40), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

while True:
    log = fake_log()
    with open(CSV_FILE, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=log.keys())
        if f.tell() == 0:
            writer.writeheader()
        writer.writerow(log)
    print("Appended to CSV:", log)
    time.sleep(60)
