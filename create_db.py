import sqlite3
# sqlite3 -csv -header stock.db "SELECT * FROM stock_company_price_daily ORDER BY CH_TIMESTAMP DESC LIMIT 10;"
# sqlite3 -header -column stock.db "SELECT * FROM stock_company_price_daily ORDER BY CH_TIMESTAMP DESC LIMIT 10;"
def create_tables():
    """
    Creates the 'stock_index_price_daily' and 'stock_company_price_daily' tables.
    """
    db_path = "stock.db"
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create the 'stock_index_price_daily' table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_index_price_daily (
                index_name TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                date_key TEXT,
                index_type TEXT,
                PRIMARY KEY (index_name, date_key)
            )
        """)
        print("Ensured 'stock_index_price_daily' table exists.")

        # Create the 'stock_company_price_daily' table if it doesn't exist
        # cursor.execute("""
        #     CREATE TABLE IF NOT EXISTS stock_company_price_daily (
        #         CH_SYMBOL TEXT,
        #         CH_SERIES TEXT,
        #         CH_TIMESTAMP TEXT,
        #         TIMESTAMP TEXT,
        #         mTIMESTAMP TEXT,
        #         CH_PREVIOUS_CLS_PRICE REAL,
        #         CH_OPENING_PRICE REAL,
        #         CH_TRADE_HIGH_PRICE REAL,
        #         CH_TRADE_LOW_PRICE REAL,
        #         CH_LAST_TRADED_PRICE REAL,
        #         CH_CLOSING_PRICE REAL,
        #         VWAP REAL,
        #         CH_TOT_TRADED_QTY INTEGER,
        #         CH_TOT_TRADED_VAL REAL,
        #         CH_TOTAL_TRADES INTEGER,
        #         CH_52WEEK_HIGH_PRICE REAL,
        #         CH_52WEEK_LOW_PRICE REAL,
        #         SLBMH_TOT_VAL REAL,
        #         index_type TEXT,
        #         index_name TEXT,
        #         PRIMARY KEY (CH_SYMBOL, CH_TIMESTAMP, index_type, index_name)
        #     )
        # """)
        # print("Ensured 'stock_company_price_daily' table exists.")

        conn.commit()

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_tables()
