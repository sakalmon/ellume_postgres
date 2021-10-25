import psycopg2
import os

POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
CSV_PATH = r'C:\My Drive\Scientific & Technical\Firmware Release\EPL Production Test Results\Tests\6171-ASM-00000546_CEQ0178.csv'

try:
    conn = psycopg2.connect(host="127.0.0.1", database="mydb", user="postgres", port="5432", password=POSTGRES_PASSWORD)
except:
    print('Unable to connect')

cur = conn.cursor()

command = """
CREATE TABLE CEQ0178 (
    test1 VARCHAR(50),
    test2 VARCHAR(50),
    test3 VARCHAR(50)
)
"""

cur.execute(command)
conn.commit()

cur.close()
conn.close()