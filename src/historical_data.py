import requests
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import time

# Constants
API_KEY = 'UG2KAVVO442WJ7OI'
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '2655',
    'database': 'atlys'
}
START_DATE = '2020-01-01'
END_DATE = '2024-05-31'
COMPANIES = ['TSCO', 'IBM', 'RELIANCE.BSE','NIKE','Autodesk']
RATE_LIMIT = 5 #no of request we can make 
WAIT_TIME = 60 / RATE_LIMIT #Wait time will be 12 seconds

conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

def create_table():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_prices (
            Date DATE,
            Company VARCHAR(20),
            Open FLOAT,
            Close FLOAT,
            High FLOAT,
            Low FLOAT,
            Volume BIGINT,
            PRIMARY KEY (Date, Company)
        )
    ''')
    conn.commit()

def fetch_data(company):
    try:
        response = requests.get(
            'https://www.alphavantage.co/query',
            params={
                'function': 'TIME_SERIES_DAILY',
                'symbol': company,
                'apikey': API_KEY
            }
        )
        response.raise_for_status()
        data = response.json()
        if 'Time Series (Daily)' not in data:
            raise ValueError(f"Data for {company} not available.")
        return data['Time Series (Daily)']
    except requests.RequestException as e:
        print(f"Error fetching data for {company}: {e}")
        return None
    except ValueError as e:
        print(e)
        return None

def process_data(data, company):
    df = pd.DataFrame.from_dict(data, orient='index')
    df.reset_index(inplace=True)
    df.rename(columns={
        'index': 'Date',
        '1. open': 'Open',
        '2. high': 'High',
        '3. low': 'Low',
        '4. close': 'Close',
        '5. volume': 'Volume'
    }, inplace=True)
    df['Company'] = company
    df['Date'] = pd.to_datetime(df['Date'])
    df = df[['Date', 'Company', 'Open', 'Close', 'High', 'Low', 'Volume']]
    # Filter by date range
    df = df[(df['Date'] >= START_DATE) & (df['Date'] <= END_DATE)]
    return df

def save_to_db(df):
    for _, row in df.iterrows():
        cursor.execute('''
            INSERT INTO stock_prices (Date, Company, Open, Close, High, Low, Volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Open = VALUES(Open),
                Close = VALUES(Close),
                High = VALUES(High),
                Low = VALUES(Low),
                Volume = VALUES(Volume)
        ''', (row['Date'], row['Company'], row['Open'], row['Close'], row['High'], row['Low'], row['Volume']))
    conn.commit()

def main():
    create_table()
    for company in COMPANIES:
        print(f"Fetching data for {company}")
        data = fetch_data(company)
        if data:
            df = process_data(data, company)
            save_to_db(df)
            print(f"Data for {company} saved to database")
        time.sleep(WAIT_TIME)

if __name__ == "__main__":
    main()

conn.close()


