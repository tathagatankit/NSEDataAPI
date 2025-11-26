import psycopg2

try:
    conn = psycopg2.connect(
        host='jvsscf0t-5432.inc1.devtunnels.ms',  # Use your forwarded URL
        port=5432,
        database='postgres',
        user='postgres',
        password='password'
    )
    print("✓ Connection successful!")
    conn.close()
except Exception as e:
    print(f"✗ Connection failed: {e}")
