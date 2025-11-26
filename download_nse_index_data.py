import requests
import json
import sqlite3
from datetime import datetime, timedelta
import argparse
import os
import pandas as pd

def get_latest_date(index_name, db_path="stock.db"):
    """Gets the latest date for a given index from the database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date_key) FROM stock_index_price_daily WHERE index_name = ?", (index_name,))
        result = cursor.fetchone()[0]
        return result
    except sqlite3.Error as e:
        print(f"Database error when fetching latest date: {e}")
        return None
    finally:
        if conn:
            conn.close()

def export_to_csv(index_name, db_path="stock.db"):
    """Exports all data for a given index from the database to a CSV file."""
    try:
        conn = sqlite3.connect(db_path)
        
        # Fetch all data for the index
        query = "SELECT date_key, open, high, low, close FROM stock_index_price_daily WHERE index_name = ? ORDER BY date_key"
        df = pd.read_sql_query(query, conn, params=(index_name,))
        
        if df.empty:
            print(f"No data found for index {index_name} to export.")
            return

        # Create directory structure
        today_date = datetime.now().strftime('%d-%m-%Y')
        file_name = f"{index_name}_01-01-2015_to_{today_date}.csv"
        dir_path = os.path.join("data", "INDEX_DATA", index_name)
        os.makedirs(dir_path, exist_ok=True)
        
        # Define the full file path
        file_path = os.path.join(dir_path, file_name)
        
        # Export to CSV
        df.to_csv(file_path, index=False)
        print(f"Successfully exported data to {file_path}")

    except sqlite3.Error as e:
        print(f"Database error during CSV export: {e}")
    except Exception as e:
        print(f"An error occurred during CSV export: {e}")
    finally:
        if conn:
            conn.close()

def fetch_and_insert_data(name, start_date, end_date, index_name):
    """
    Fetches stock data from the API, transforms it, and inserts it into the database.
    """
    api_url = "https://www.niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString"
    cinfo_payload = {
        "name": name,
        "startDate": start_date,
        "endDate": end_date,
        "indexName": index_name
    }
    # The API expects the cinfo value to be a string that looks like a dictionary, using single quotes.
    request_body = {
        "cinfo": json.dumps(cinfo_payload).replace('"', "'")
    }
    # print("Request Body:", request_body)
    db_path = "stock.db"
    conn = None

    try:
        # Fetch data from the API
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.post(api_url, json=request_body, headers=headers, timeout=100)
        response.raise_for_status()  # Raise an exception for bad status codes

        # The response['d'] is a JSON string, so it needs to be parsed
        data = json.loads(response.json()['d'])
        print(f"Fetched {len(data)} records from the API.")
        print("Sample record:", data[0] if data else "No data")
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Prepare data for insertion
        data_to_insert = []
        for record in data:
            # Transform date format
            try:
                date_obj = datetime.strptime(record['HistoricalDate'], '%d %b %Y')
                date_key = date_obj.strftime('%Y-%m-%d')
            except ValueError:
                print(f"Could not parse date: {record['HistoricalDate']}. Skipping row.")
                continue

            data_to_insert.append((
                record['INDEX_NAME'].upper(),
                float(record['OPEN']),
                float(record['HIGH']),
                float(record['LOW']),
                float(record['CLOSE']),
                date_key,
                'sectoral'  # Adding the index_type
            ))

        # Insert data into the tables
        inserted_count = 0
        for record in data_to_insert:
            try:

                # Insert into stock_index_price_daily
                cursor.execute("""
                    INSERT INTO stock_index_price_daily (index_name, open, high, low, close, date_key, index_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, record)

                inserted_count += 1
            except sqlite3.IntegrityError:
                # This error occurs if the primary key (index_name, date_key) already exists.
                print(f"Skipping duplicate record for {record[0]} on {record[5]}.")

        conn.commit()
        print(f"Successfully inserted {inserted_count} new rows into the database.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except (ValueError, KeyError) as e:
        print(f"Error processing data: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch historical stock data.")
    parser.add_argument("--index", type=str, required=True, help="The name of the index to fetch.")
    args = parser.parse_args()

    INDEX = args.index.upper()
    
    # Determine the start date
    latest_date_str = get_latest_date(INDEX)
    if latest_date_str:
        latest_date = datetime.strptime(latest_date_str, '%Y-%m-%d')
        start_date_obj = latest_date + timedelta(days=1)
        start_date = start_date_obj.strftime('%d-%b-%Y')
    else:
        # If no data exists, start from a default date
        start_date = '01-Jan-2015'

    # End date is always today
    end_date = datetime.now().strftime('%d-%b-%Y')

    print(f"Fetching data for {INDEX} from {start_date} to {end_date}")

    fetch_and_insert_data(name=INDEX,
                          start_date=start_date,
                          end_date=end_date,
                          index_name=INDEX)
                          
    # Export the data to CSV
    export_to_csv(INDEX)
