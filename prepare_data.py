import os
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
CSV_DIR = r'C:\My Drive\Scientific & Technical\Firmware Release\EPL Production Test Results\Tests'

engine = create_engine(f'postgresql://postgres:{POSTGRES_PASSWORD}@localhost:5432/mydb')

for file in os.listdir(CSV_DIR):
    if file.startswith('6171-ASM') and file.endswith('.csv'):
        renamed_columns = []

        header = pd.read_csv(os.path.join(CSV_DIR, file), error_bad_lines=False)

        df = pd.read_csv(os.path.join(CSV_DIR, file), header=2)

        for column in header.columns:
            df[column] = header[column][0]

        df['Start'] = pd.to_datetime(df['Start'])
        df['Finish'] = pd.to_datetime(df['Finish'])
        df['Product Expiry'] = pd.to_datetime(df['Product Expiry'])

        df.to_sql("results", engine, if_exists='append')


        
        
    
        

