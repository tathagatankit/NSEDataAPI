import requests
import json
import sqlite3
from datetime import datetime

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

        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_index_price_daily (
                index_name TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                date_key TEXT,
                PRIMARY KEY (index_name, date_key)
            )
        """)

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
                date_key
            ))

        # Insert data into the table
        inserted_count = 0
        for record in data_to_insert:
            try:
                cursor.execute("""
                    INSERT INTO stock_index_price_daily (index_name, open, high, low, close, date_key)
                    VALUES (?, ?, ?, ?, ?, ?)
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
    # Example usage:
    # INDEX = 'NIFTY CHEMICALS'
    # INDEX = 'NIFTY CONSUMER DURABLES'
    INDEX = 'Nifty Pharma'
    INDEXES = ['NIFTY500 FLEXICAP', 'Nifty Pvt Bank', 'Nifty 200', 'Nifty 50', 'Nifty 500', 'Nifty Auto', 'Nifty Bank', 'Nifty Commodities', 'Nifty Consumption', 'Nifty CPSE', 
               'Nifty Div Opps 50', 'Nifty Energy', 'Nifty Fin Service', 'Nifty FMCG', 'Nifty GrowSect 15', 
               'Nifty Infra', 'Nifty IT', 'Nifty Media', 'Nifty Metal', 'Nifty Mid Liq 15', 
               'NIFTY MIDCAP 100', 'Nifty Midcap 50', 'Nifty MNC', 'Nifty Next 50', 'Nifty Pharma', 
               'Nifty PSE', 'Nifty PSU Bank', 'Nifty 100', 'NIFTY100 QUALTY30', 'Nifty Realty', 
               'Nifty Serv Sector', 'NIFTY SMLCAP 100', 'Nifty100 Liq 15', 'Nifty50 Div Point', 
               'Nifty50 PR 1x Inv', 'Nifty50 PR 2x Lev', 'Nifty50 TR 1x Inv', 'Nifty50 TR 2x Lev', 
               'Nifty50 Value 20', 'NIFTY 1D RATE INDEX', 'NIFTY 50 ARBITRAGE', 'NIFTY 50 FUTURES INDEX', 
               'NIFTY 50 FUTURES TR INDEX', 'NIFTY ADITYA BIRLA GROUP', 'NIFTY ALPHA 50', 
               'NIFTY FULL MIDCAP 100', 'NIFTY FULL SMALLCAP 100', 
               'Nifty HighBeta 50', 'Nifty Low Vol 50', 'NIFTY MAHINDRA GROUP', 'NIFTY MIDCAP 150', 'Nifty Shariah 25', 'NIFTY SMLCAP 250', 
               'NIFTY SMLCAP 50', 'NIFTY TATA GROUP', 'NIFTY TATA 25 CAP', 'NIFTY100 EQL WGT', 'NIFTY100 LOWVOL30',
                'Nifty50 Shariah', 'NIFTY50 USD', 'Nifty500 Shariah', 'NIFTY 5YR BENCHMARK G-SEC INDEX', 
                'Nifty GS 10Yr', 'Nifty GS 10Yr Cln', 'Nifty GS 11 15Yr', 'Nifty GS 15YrPlus', 'Nifty GS 4 8Yr', 
                'Nifty GS 8 13Yr', 'Nifty GS Compsite', 'NIFTY AAA CORPORATE BOND', 'NIFTY AAA LONG-TERM CORPORATE BOND', 
                'NIFTY AAA MEDIUM-TERM CORPORATE BOND', 'NIFTY AAA SHORT-TERM CORPORATE BOND', 'NIFTY AAA ULTRA LONG-TERM CORPORATE BOND', 
                'NIFTY AAA ULTRA SHORT-TERM CORPORATE BOND', 'NIFTY ALPHALOWVOL', 'Nifty AQL 30', 'Nifty Qlty LV 30', 'Nifty AQLV 30', 
                'NIFTY50 EQL WGT', 'NIFTY 50 BLENDED 10 YR BENCHMARK G-SEC - GROWTH INDEX', 'NIFTY NEXT 50 BLENDED 10 YR BENCHMARK G-SEC - GROWTH INDEX', 
                'NIFTY 10 YEAR SDL INDEX', 'NIFTY SME EMERGE', 'Nifty500 Value 50', 'NIFTY200 QUALITY 30', 'Nifty100 Enh ESG', 'NIFTY100 ESG', 'Nifty BHARAT Bond Index - April 2023', 
                'BHARATBOND-APR30', 'BHARATBOND-APR25', 'BHARATBOND-APR31', 'NIFTY M150 QLTY50', 'NIFTY FINSRV25 50', 'Nifty100ESGSecLdr', 'Nifty200Momentm30', 'Nifty100 Alpha 30', 
                'Nifty CPSE Bond Plus SDL Sep 2024 50:50 Index', 'NIFTY HEALTHCARE', 'NIFTY MIDSML 400', 'NIFTY500 MULTICAP', 'Nifty PSU Bond Plus SDL Apr 2026 50:50 Index', 
                'Nifty AAA Bond Plus SDL Apr 2026 50:50 Index', 'NIFTY MICROCAP250', 'NIFTY INDIA MFG', 'Nifty New Consump', 'Nifty SDL Apr 2026 Top 20 Equal Weight Index', 
                'Nifty India Government Fully Accessible Route (FAR) Select 7 Bonds Index (USD)', 'Nifty India Government Fully Accessible Route (FAR) Select 7 Bonds Index (INR)', 
                'NIFTY CONSR DURBL', 'NIFTY OIL AND GAS', 'NIFTY LARGEMID250', 'Nifty SDL Plus PSU Bond Sep 2026 60:40 Index', 'NIFTY MID SELECT', 'Nifty PSU Bond Plus SDL Sep 2027 40:60 Index', 
                'Nifty PSU Bond Plus SDL Apr 2027 50:50 Index', 'NIFTY AAA BOND PLUS SDL APR 2031 70:30 INDEX', 'NIFTY AAA BOND PLUS SDL APR 2026 70:30 INDEX', 'NIFTY TOTAL MKT', 'Nifty NonCyc Cons', 
                'BHARATBOND-APR32', 'Nifty Mobility', 'NIFTY IND DIGITAL', 'NIFTY INTERNET', 'Nifty CPSE Bond Plus SDL Sep 2026 50:50 Index', 'Nifty SDL Apr 2027 Index', 'Nifty Ind Defence', 
                'Nifty SDL Apr 2027 Top 12 Equal Weight Index', 'Nifty SDL Apr 2032 Top 12 Equal Weight Index', 'Nifty FinSerExBnk', 'Nifty Housing', 'Nifty Trans Logis', 
                'Nifty SDL Plus G-Sec Jun 2028 30:70 Index', 'Nifty SDL Plus AAA PSU Bond Dec 2027 60:40 Index', 'Nifty SDL Jun 2027 Index', 'Nifty SDL Sep 2027 Index', 
                'Nifty AAA CPSE Bond Plus SDL Apr 2027 60:40 Index', 'Nifty G-Sec Jun 2027 Index', 'Nifty AQLV 30 Plus 5yr G-Sec 70:30 index', 'Nifty Fixed Income PRC Indices', 
                'Nifty200 Alpha 30', 'Nifty MS Fin Serv', 'NIFTY MIDSML HLTH', 'Nifty MS IT Telcm', 'NIFTYM150MOMNTM50', 'Nifty SDL Sep 2025 Index', 'Nifty SDL Dec 2028 Index', 'Nifty SDL Plus AAA PSU Bond Jul 2028 60:40 Index', 'Nifty G-Sec Dec 2030 Index', 'Nifty SDL Plus AAA PSU Bond Jul 2033 60:40 Index', 'Nifty AAA PSU Bond Plus SDL Sep 2026 50:50 Index', 'Nifty MS Ind Cons', 'Nifty G-Sec Dec 2026 Index', 'Nifty G-Sec Jul 2031 Index', 'Nifty SDL Sep 2026 Index', 'Nifty SDL Plus G-Sec Jun 2028 70:30 Index', 'Nifty G-Sec Sep 2027 Index', 'Nifty G-Sec Jun 2036 Index', 'Nifty G-Sec Sep 2032 Index', 'BHARATBOND-APR33', 'Nifty SDL Sep 2026 V1 Index', 'Nifty SDL Jul 2026 Index', 'Nifty G-Sec Dec 2029 Index', 'Nifty AAA PSU Bond Plus SDL Apr 2026 50:50 Index', 'Nifty SDL Dec 2026 Index', 'Nifty SDL Plus G-Sec Sep 2027 50:50 Index', 'Nifty SDL Plus AAA PSU Bond Apr 2026 75:25 Index', 'Nifty SDL Plus G-Sec Jun 2029 70:30 Index', 'Nifty SDL Jul 2033 Index', 'Nifty India Sovereign Green Bond Dec 2033 Index', 'Nifty India Sovereign Green Bond June 2028 Index', 'Nifty G-Sec Oct 2028 Index', 'Nifty SDL Oct 2026 Index', 'Nifty India Municipal Bond Index', 'Nifty G-Sec Apr 2029 Index', 'Nifty SDL Plus AAA PSU Bond Apr 2028 75:25 Index', 'Nifty G-Sec May 2029 Index', 'Nifty India Sovereign Green Bond Jan 2033 Index', 'Nifty India Sovereign Green Bond Jan 2028 Index', 'Nifty SDL Plus G-Sec June 2027 40:60 Index', 'Nifty SDL Jul 2028 Index', 'Nifty G-Sec Jul 2027 Index', 'Nifty Sml250 Q50', 'Nifty SDL June 2028 Index', 'Nifty REITs & InvITs', 'Nifty Multi Asset - Equity : Arbitrage : REITS/INVITS (50:40:10) Index', 'Nifty Multi Asset - Equity : Debt : Arbitrage : REITS/INVITS (50:20:20:10) Index', 'Nifty CoreHousing', 'Nifty 5 Year SDL Index', 'Nifty 3 Year SDL Index', 'Nifty G-Sec Jul 2033 Index', 'NiftySml250MQ 100', 'NiftyMS400 MQ 100', 'NIFTY MULTI MFG', 'NIFTY MULTI INFRA', 'Nifty AAA Bond Jun 2025 HTM Index', 'Nifty India Corporate Group Index - Aditya Birla Group', 'Nifty India Corporate Group Index - Mahindra Group', 'Nifty India Corporate Group Index - Tata Group', 'Nifty EV', 'Nifty500 EW', 'Nifty500Momentm50', 'Nifty500 LMS Eql', 'Nifty200 Value 30', 'Nifty Ind Tourism', 'Nifty Top 10 EW', 'Nifty IPO', 'Nifty Rural', 'Nifty Multi MQ 50', 'Nifty Capital Mkt', 'Nifty Top 15 EW', 'Nifty Top 20 EW', 'Nifty Corp MAATR', 'Nifty India Railways PSU', 'Nifty500 Qlty50', 'Nifty500 LowVol50', 'Nifty500 MQVLv50', 'Nifty AAA Financial Services Bond Mar 2028 Index', 'Nifty AAA Bond Plus G-Sec Mar 2035 30:70 Index', 'Nifty Chemicals', 'Nifty Waves', 'Nifty AAA Financial Services Bond Plus G-Sec Apr 2028 90:10 Index', 'Nifty AAA Financial Services Bond Plus G-Sec Apr 2027 90:10 Index', 'NIFTY INFRALOG', 'Nifty Financial Services 3 to 6 Months Debt Index', 'Nifty Financial Services 9 to 12 Months Debt Index', 'NIFTY500 HEALTH', 'NIFTY TMMQ 50', 'NIFTY FPI 150', 'Nifty Conglomerate 50']
    fetch_and_insert_data(name=INDEX,
                          start_date='01-Jan-2015',
                          end_date='17-Nov-2025',
                          index_name=INDEX)
