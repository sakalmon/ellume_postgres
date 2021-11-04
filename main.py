import psycopg2
import os
import re
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
CSV_DIR = r'C:\My Drive\Scientific & Technical\Firmware Release\EPL Production Test Results\Tests'

# For locating CEQ number
CEQ_MAP = {
    'AR1': {
        'Line1': 'CEQ0175',
        'Line2': 'CEQ0176',
        'Line3': 'CEQ0271',
        'Line4': 'CEQ0275',
        'Line5': 'CEQ0177',
        'Line6': 'CEQ0179'
    },
    'AR2': {
        'Line1': 'CEQ0178',
        'Line2': 'CEQ0181',
        'Line3': 'CEQ0180',
        'Line4': 'CEQ0192',
        'Line5': 'CEQ0215',
        'Line6': 'CEQ0214'
    },
    'AR3': {
        'Line1': 'CEQ0221',
        'Line2': 'CEQ0212',
        'Line3': 'CEQ0217'
    }
}

pattern = re.compile(r'-|\s|[.]')

try:
    conn = psycopg2.connect(host="127.0.0.1", database="mydb", user="postgres",\
                            port="5432", password=POSTGRES_PASSWORD)
except:
    print('Unable to connect')

cur = conn.cursor()

engine = create_engine(f'postgresql://postgres:{POSTGRES_PASSWORD}@localhost:5432/mydb')

for file in os.listdir(CSV_DIR):
    if file.startswith('6171-ASM') and file.endswith('.csv'):
        renamed_columns = []

        df = pd.read_csv(os.path.join(CSV_DIR, file), header=2)

        columns = list(df.columns)

        for column in columns:
            matches = pattern.findall(column)
                
            # if matches:
            #     temp = []
            #     temp.append(column.replace(matches[0], '_'))

            #     for match in matches:
            #         temp.append(temp.pop().replace(match, '_'))
                
            #     renamed_columns.append(temp[0])
            
            # else:
            renamed_columns.append(column)

        renamed_columns = ' VARCHAR(50), '.join(renamed_columns) + ' VARCHAR(50)'
        table = file.replace('-', '_')
        table = table[:-4]
        # sql = (f'CREATE TABLE "{table}"({renamed_columns});')

        # cur.execute(sql)
        # conn.commit()
        # cur.close()
        # conn.close()
        print(f'Adding to table: {table}')
        df.to_sql(table, engine, if_exists='append', index=False)
        

