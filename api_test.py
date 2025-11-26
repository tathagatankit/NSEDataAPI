import requests
import json
from datetime import datetime, timedelta

def test_nse_api():
    """
    Tests the NSE historical data API to understand its response structure and limitations.
    """
    symbol = "MARUTI"
    series = "EQ"
    # A date range longer than the usual limit to test pagination
    from_date = "01-06-2025"
    to_date = "23-11-2025"

    base_url = "https://www.nseindia.com/api/historical/cm/equity"
    url = f"{base_url}?symbol={symbol}&series=[%22{series}%22]&from={from_date}&to={to_date}"

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}',
        'X-Requested-With': 'XMLHttpRequest'
    }

    try:
        session = requests.Session()
        # Initial request to get cookies
        session.get("https://www.nseindia.com", headers=headers)
        
        # API request
        response = session.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()

        # Print the full JSON response for analysis
        print(json.dumps(data, indent=4))

        if 'data' in data:
            print(f"\nNumber of data points received: {len(data['data'])}")
        else:
            print("\n'data' key not found in the response.")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    except json.JSONDecodeError:
        print("Failed to decode JSON. Response content:")
        print(response.text)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    test_nse_api()
