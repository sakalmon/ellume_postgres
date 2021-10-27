import psycopg2
import os
import re
import pandas as pd

POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
columns = list(pd.read_csv(r'C:\Users\sakal.mon\OneDrive - Ellume\Documents\Git\ellume_postgres\6171-ASM-00000546_CEQ0178.csv', header=2).columns)
renamed_columns = []

pattern = re.compile(r'-|\s|[.]')
for column in columns:
    matches = pattern.finditer(column)
    for match in matches:
        renamed_columns.append(column.replace(match, '_'))

print(renamed_columns)
# try:
#     conn = psycopg2.connect(host="127.0.0.1", database="mydb", user="postgres",\
#                             port="5432", password=POSTGRES_PASSWORD)
# except:
#     print('Unable to connect')

# cur = conn.cursor()

# for column in renamed_columns:
#     command = f"""
#         ALTER TABLE CEQ0178
#         ADD COLUMN {column} VARCHAR(50)
#     """
#     cur.execute(command)
#     conn.commit()



# cur.close()
# conn.close()