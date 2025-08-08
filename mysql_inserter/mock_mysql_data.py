import os
import time
import random
from faker import Faker
import pymysql

fake = Faker()
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "rootpassword")
MYSQL_DB = os.getenv("MYSQL_DB", "sensor_data")

def init_conn():
    return pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT,
                           user=MYSQL_USER, password=MYSQL_PASS,
                           database=MYSQL_DB, autocommit=True)

TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sensor_data_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(64),
    temperature FLOAT,
    humidity FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

INSERT_SQL = "INSERT INTO sensor_data_table (device_id, temperature, humidity) VALUES (%s,%s,%s)"

while True:
    try:
        conn = init_conn()
        with conn.cursor() as cur:
            cur.execute(TABLE_SQL)
            device = fake.word() + str(random.randint(1,100))
            temp = round(random.uniform(-10,40),2)
            hum = round(random.uniform(10,90),2)
            cur.execute(INSERT_SQL, (device, temp, hum))
            print(f"Inserted {device}, {temp}, {hum}")
        conn.close()
        time.sleep(60)
    except Exception as e:
        print("Waiting for MySQL...", e)
        time.sleep(10)
