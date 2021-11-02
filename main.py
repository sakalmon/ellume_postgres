import psycopg2
import os
import re
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
columns = list(pd.read_csv(r'C:\Users\sakal.mon\OneDrive - Ellume\Documents\Git\postgres\6171-ASM-00000546_CEQ0178.csv', header=2).columns)
renamed_columns = []

pattern = re.compile(r'-|\s|[.]')

for column in columns:
    matches = pattern.findall(column)
    
    if matches:
        temp = []
        temp.append(column.replace(matches[0], '_'))

        for match in matches:
            temp.append(temp.pop().replace(match, '_'))

        renamed_columns.append(temp[0])

    else:
        renamed_columns.append(column)

print(renamed_columns)

try:
    conn = psycopg2.connect(host="127.0.0.1", database="mydb", user="postgres",\
                            port="5432", password=POSTGRES_PASSWORD)
except:
    print('Unable to connect')

cur = conn.cursor()

engine = create_engine(f'postgresql://postgres:{POSTGRES_PASSWORD}@localhost:5432/mydb')

df = pd.read_csv(r'C:\Users\sakal.mon\OneDrive - Ellume\Documents\Git\postgres\6171-ASM-00000546_CEQ0178.csv', header=2, names=renamed_columns)

df.to_sql('ceq0178', engine, if_exists='append')