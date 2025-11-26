# NSE Data Ingestion and Dashboard

This project provides a web-based dashboard to fetch, store, and visualize historical stock index data from the National Stock Exchange (NSE) of India.

## Features

- **Interactive Dashboard**: A user-friendly web interface built with Streamlit.
- **Data Summary**: View a summary of the existing data in the database, including the date range for each index.
- **Data Fetching**: Select from a comprehensive list of NSE indices and fetch historical data.
- **Real-time Logging**: Monitor the data fetching process with real-time logs displayed on the dashboard.
- **SQLite Database**: Data is stored locally in a SQLite database for persistence and easy access.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/tathagatankit/NSEDataAPI.git
    cd NseDataIngestion
    ```

2.  **Create a virtual environment and activate it:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  **Run the Streamlit dashboard:**
    ```bash
    streamlit run dashboard.py
    ```

2.  **Open your web browser** and navigate to the URL provided by Streamlit (usually `http://localhost:8501`).

3.  **Fetch Data:**
    -   Select a stock index from the dropdown menu.
    -   Click the "Fetch Data" button to download and store the historical data for the selected index.

## Project Structure

-   `dashboard.py`: The main Streamlit application file.
-   `fetch_and_insert.py`: Script responsible for fetching data from the NSE API and inserting it into the database.
-   `create_db.py`: Script to initialize the SQLite database and create the necessary tables.
-   `stock.db`: The SQLite database file where the stock data is stored.
-   `requirements.txt`: A list of Python dependencies for the project.
