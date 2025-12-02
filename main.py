import yfinance as yf
import json
import argparse
import logging
from pathlib import Path
import sys

# --- Configuration ---

# Set up basic logging to print info and error messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Output logs to the console
    ]
)

# Define a directory to save the output files
OUTPUT_DIR = Path("historical_data")

# --- Core Functions ---

def load_config(config_path: Path) -> list:
    """
    Loads the list of ticker requests from a JSON configuration file.

    Args:
        config_path: The Path object pointing to the JSON config file.

    Returns:
        A list of dictionaries, where each dictionary contains
        ticker, start_date, and end_date.
    """
    if not config_path.exists():
        logging.error(f"Configuration file not found: {config_path}")
        sys.exit(1)  # Exit the script with an error code

    try:
        with open(config_path, 'r') as f:
            config_data = json.load(f)

        # Basic validation
        if not isinstance(config_data, list):
            raise ValueError("Configuration JSON must contain a list of requests.")

        for item in config_data:
            if not all(key in item for key in ["ticker", "start_date", "end_date"]):
                raise ValueError(f"Invalid item in config: {item}. Must contain 'ticker', 'start_date', and 'end_date'.")

        logging.info(f"Successfully loaded {len(config_data)} requests from {config_path}")
        return config_data

    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON from {config_path}. Please check the file format.")
        sys.exit(1)
    except ValueError as e:
        logging.error(f"Invalid configuration: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading the config: {e}")
        sys.exit(1)

def fetch_and_save_data(ticker_request: dict, output_dir: Path):
    """
    Fetches data for a single ticker and saves it to a CSV file.

    Args:
        ticker_request: A dictionary with "ticker", "start_date", "end_date".
        output_dir: The directory to save the CSV file in.
    """
    ticker_str = ticker_request["ticker"]
    start = ticker_request["start_date"]
    end = ticker_request["end_date"]

    logging.info(f"Fetching data for {ticker_str} from {start} to {end}...")

    try:
        # 1. Create a Ticker object
        ticker_obj = yf.Ticker(ticker_str)

        # 2. Fetch historical data
        # yfinance automatically handles date formatting
        history_df = ticker_obj.history(start=start, end=end)

        # 3. Check if data was returned
        if history_df.empty:
            logging.warning(f"No data found for {ticker_str} in the given date range.")
            return

        # 4. Create a clean filename
        # Replaces characters that are invalid in filenames
        safe_ticker_str = ticker_str.replace(".", "_")
        filename = f"{safe_ticker_str}_{start}_to_{end}.csv"
        output_path = output_dir / filename

        # 5. Save the data to CSV
        history_df.to_csv(output_path)
        logging.info(f"Successfully saved data to {output_path}")

    except Exception as e:
        # Handle potential errors (e.g., network issues, invalid ticker)
        logging.error(f"Failed to fetch or save data for {ticker_str}: {e}")

def main():
    """
    Main function to parse arguments, load config, and process requests.
    """
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(
        description="Fetch historical stock data from Yahoo Finance and save to CSV."
    )
    parser.add_argument(
        "-c", "--config",
        type=Path,
        default=Path("config.json"),  # Default config file name
        help="Path to the JSON configuration file. (default: config.json)"
    )
    args = parser.parse_args()

    # --- Load Configuration ---
    requests = load_config(args.config)

    # --- Prepare Output Directory ---
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        logging.info(f"Output directory set to: {OUTPUT_DIR.resolve()}")
    except OSError as e:
        logging.error(f"Failed to create output directory {OUTPUT_DIR}: {e}")
        sys.exit(1)

    # --- Process Requests ---
    if not requests:
        logging.warning("No data fetch requests found in the configuration.")
        return

    for request in requests:
        fetch_and_save_data(request, OUTPUT_DIR)

    logging.info("All data fetching tasks complete.")

# --- Script Entry Point ---
if __name__ == "__main__":
    main()