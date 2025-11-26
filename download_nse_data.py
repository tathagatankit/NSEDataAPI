import requests
import pandas as pd
from datetime import datetime
import sqlite3
import os

def insert_data_to_db(data, index_name):
    """
    Inserts data directly from API response into the stock_company_price_daily table.

    Args:
        data (list): A list of dictionaries, where each dictionary is a row of data.
    """
    db_path = "stock.db"
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        data_to_insert = []
        for row in data:
            # Handle empty strings for numeric fields
            for key in ['CH_PREVIOUS_CLS_PRICE', 'CH_OPENING_PRICE', 'CH_TRADE_HIGH_PRICE', 'CH_TRADE_LOW_PRICE', 'CH_LAST_TRADED_PRICE', 'CH_CLOSING_PRICE', 'VWAP', 'CH_TOT_TRADED_QTY', 'CH_TOT_TRADED_VAL', 'CH_TOTAL_TRADES', 'CH_52WEEK_HIGH_PRICE', 'CH_52WEEK_LOW_PRICE', 'SLBMH_TOT_VAL']:
                if row[key] == '':
                    row[key] = None
            
            data_to_insert.append((
                row['CH_SYMBOL'], row['CH_SERIES'], row['CH_TIMESTAMP'], row['TIMESTAMP'], row['mTIMESTAMP'],
                row['CH_PREVIOUS_CLS_PRICE'], row['CH_OPENING_PRICE'], row['CH_TRADE_HIGH_PRICE'],
                row['CH_TRADE_LOW_PRICE'], row['CH_LAST_TRADED_PRICE'], row['CH_CLOSING_PRICE'], row['VWAP'],
                row['CH_TOT_TRADED_QTY'], row['CH_TOT_TRADED_VAL'], row['CH_TOTAL_TRADES'],
                row['CH_52WEEK_HIGH_PRICE'], row['CH_52WEEK_LOW_PRICE'], row['SLBMH_TOT_VAL'],
                'sectoral',
                index_name
            ))

        inserted_count = 0
        for record in data_to_insert:
            try:
                cursor.execute("""
                    INSERT INTO stock_company_price_daily (
                        CH_SYMBOL, CH_SERIES, CH_TIMESTAMP, TIMESTAMP, mTIMESTAMP,
                        CH_PREVIOUS_CLS_PRICE, CH_OPENING_PRICE, CH_TRADE_HIGH_PRICE,
                        CH_TRADE_LOW_PRICE, CH_LAST_TRADED_PRICE, CH_CLOSING_PRICE, VWAP,
                        CH_TOT_TRADED_QTY, CH_TOT_TRADED_VAL, CH_TOTAL_TRADES,
                        CH_52WEEK_HIGH_PRICE, CH_52WEEK_LOW_PRICE, SLBMH_TOT_VAL, index_type, index_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, record)
                inserted_count += 1
            except sqlite3.IntegrityError:
                print(f"Skipping duplicate record for {record[0]} on {record[2]}.")

        conn.commit()
        print(f"Successfully inserted {inserted_count} rows into the database.")

    except sqlite3.Error as e:
        print(f"Database error during insertion: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during insertion: {e}")
    finally:
        if conn:
            conn.close()

def download_nse_data(index, index_name,symbol, from_date, to_date, series="EQ"):
    """
    Downloads historical data for a given symbol from the NSE India API.
    Splits the date range into 60-day chunks to bypass API limitations and saves to a single CSV.

    Args:
        index (str): The index name (e.g., "NIFTY_AUTO").
        symbol (str): The stock symbol (e.g., "MARUTI").
        from_date (str): The start date in DD-MM-YYYY format.
        to_date (str): The end date in DD-MM-YYYY format.
        series (str, optional): The series type. Defaults to "EQ".
    """
    from_dt = datetime.strptime(from_date, "%d-%m-%Y")
    to_dt = datetime.strptime(to_date, "%d-%m-%Y")
    
    current_dt = from_dt
    all_data = []

    while current_dt <= to_dt:
        chunk_end_dt = current_dt + pd.DateOffset(days=60)
        if chunk_end_dt > to_dt:
            chunk_end_dt = to_dt
            
        from_chunk = current_dt.strftime("%d-%m-%Y")
        to_chunk = chunk_end_dt.strftime("%d-%m-%Y")
        
        print(f"Fetching data for {symbol} from {from_chunk} to {to_chunk}...")
        
        base_url = "https://www.nseindia.com/api/historical/cm/equity"
        url = f"{base_url}?symbol={symbol}&series=[%22{series}%22]&from={from_chunk}&to={to_chunk}"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}',
            'X-Requested-With': 'XMLHttpRequest'
        }

        try:
            session = requests.Session()
            session.get("https://www.nseindia.com", headers=headers)
            
            response = session.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()

            if 'data' in data and data['data']:
                all_data.extend(data['data'])
                insert_data_to_db(data['data'], index_name)
            else:
                print(f"No data found for the period {from_chunk} to {to_chunk}.")

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            
        current_dt = chunk_end_dt + pd.DateOffset(days=1)

    if all_data:
        combined_df = pd.DataFrame(all_data)
        combined_df = combined_df.sort_values(by='CH_TIMESTAMP', ascending=False)
        
        folder = os.path.join("data", index, symbol.upper())
        os.makedirs(folder, exist_ok=True)
        
        filename = os.path.join(folder, f"{symbol}_{from_date}_to_{to_date}.csv")
        
        combined_df.to_csv(filename, index=False)
        print(f"Successfully downloaded all data and saved to {filename}")


if __name__ == "__main__":
    # --- Parameters to Change ---
    stock_series = "EQ"
    years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    nifty_auto = ["MARUTI", "TMPV", "M&M", "HEROMOTOCO", "EICHERMOT", "BAJAJ-AUTO", "UNOMINDA", "SONACOMS", "ASHOKLEY", "TVSMOTOR", "BOSCHLTD", "EXIDEIND", "BHARATFORG", "MOTHERSON", "TIINDIA"]
    nifty_financial_services = ["SBICARD", "HDFCLIFE", "SBILIFE", "SHRIRAMFIN", "KOTAKBANK", "AXISBANK", "PFC", "RECLTD", "SBIN", "ICICIBANK", "HDFCBANK", "ICICIPRULI", "BSE", "LICHSGFIN", "JIOFIN", "MUTHOOTFIN", "ICICIGI", "BAJAJFINSV", "BAJFINANCE", "CHOLAFIN"]
    nifty_fmcg = ["TATACONSUM", "ITC", "UNITDSPR", "HINDUNILVR", "MARICO", "COLPAL", "BRITANNIA", "NESTLEIND", "GODREJCP", "UBL", "VBL", "PATANJALI", "EMAMILTD", "DABUR", "RADICO"]
    nifty_it = ["INFY", "TECHM", "TCS", "MPHASIS", "WIPRO", "PERSISTENT", "LTIM", "HCLTECH", "OFSS", "COFORGE"]
    nifty_metal = ["APLAPOLLO", "ADANIENT", "NMDC", "WELCORP", "JSL", "TATASTEEL", "JINDALSTEL", "NATIONALUM", "HINDALCO", "VEDL", "JSWSTEEL", "HINDZINC", "SAIL", "LLOYDSME", "HINDCOPPER"]
    
    indexes = nifty_metal
    index = "NIFTY_METAL"
    index_name = index.replace("_", " ")
    for ind in indexes:
        stock_symbol = ind # Change index to fetch data for different stocks
        
        # Record start time for the entire loop
        loop_start_time = datetime.now()
        for year in years:
            start_date = f"01-01-{year}"
            end_date = f"31-12-{year}"
            print(f"Fetching data for {stock_symbol} from {start_date} to {end_date}...")
            download_nse_data(index, index_name,stock_symbol, start_date, end_date, stock_series)

    # Record end time and calculate total
    loop_end_time = datetime.now()
    total_loop_time = str(loop_end_time - loop_start_time)
    print(f"Total time taken for all years: {total_loop_time}")

        
