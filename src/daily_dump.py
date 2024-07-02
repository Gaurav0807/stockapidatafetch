import requests
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
import time

# Constants
API_KEY = 'UG2KAVVO442WJ7OI'
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = '2655'
DB_NAME = 'atlys'
COMPANIES = ['TSCO', 'IBM', 'RELIANCE.BSE','NIKE','CRED','AUTODESK']
RATE_LIMIT = 5
WAIT_TIME = 60 / RATE_LIMIT 



conn = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)
cursor = conn.cursor()

def create_table():
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS stock_prices_daily_dump (
        Date DATE,
        Company VARCHAR(50),
        Open FLOAT,
        Close FLOAT,
        High FLOAT,
        Low FLOAT,
        Volume BIGINT,
        PRIMARY KEY (Date, Company)
    )
    '''
    cursor.execute(create_table_query)
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
            raise ValueError(f"Data for {company} not available or invalid response format.")
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
    yesterday_date = datetime.now() - timedelta(days=1)
    yesterday_date_format = yesterday_date.strftime('%Y-%m-%d')
    df = df[df['Date'] == yesterday_date_format]
    return df

def save_to_db(df):
    for _, row in df.iterrows():
        insert_query = '''
        INSERT INTO stock_prices_daily_dump (Date, Company, Open, Close, High, Low, Volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        Open = VALUES(Open),
        Close = VALUES(Close),
        High = VALUES(High),
        Low = VALUES(Low),
        Volume = VALUES(Volume)
        '''
        cursor.execute(insert_query, tuple(row))
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
