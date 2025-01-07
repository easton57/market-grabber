"""
Market helper using yfinance
"""

import logging
import pandas as pd
import yfinance as yf
import statistics as stats
from datetime import datetime, timedelta
from postgres_helper import PsSQLHelper as pgh


def download(tick_name, start_date=f"{(datetime.now()  - timedelta(days=6)).strftime('%Y-%m-%d')}",
             end_date=f"{datetime.now().strftime('%Y-%m-%d')}", interval="5m", today=False, full=False) -> bool:
    logging.basicConfig(filename=f"logs/download_{tick_name}_{datetime.today().strftime('%Y-%m-%d')}.log", filemode='a',
                        level=logging.DEBUG, force=True, format='[%(asctime)s] %(name)s %(levelname)s - %(message)s')

    """ Will download the ticker data, do some calculations and add it to the database """
    # if interval returns an error handle it gracefully and inform the user of how much data they'll be getting
    try:
        if today:
            data = yf.download(tick_name, period='1d', interval=interval)
        elif full:
            if interval == '1m':
                # Has to be the last 7 days
                day = 7
                start_date = (datetime.today() - timedelta(days=6)).strftime('%Y-%m-%d')
            elif interval in ['2m', '5m', '15m', '30m', '90m']:
                # These are the last 60 days
                day = 60
                start_date = (datetime.today() - timedelta(days=59)).strftime('%Y-%m-%d')
            elif interval in ['60m', '1h']:
                # These have a phat delta of 730 days
                day = 730
                start_date = (datetime.today() - timedelta(days=729)).strftime('%Y-%m-%d')
            else:
                logging.error("Interval invalid! Please refer to help for appropriate interval times")
                return False

            data = yf.download(tick_name, start=start_date, end=end_date, interval=interval)
        else:
            data = yf.download(tick_name, start=start_date, end=end_date, interval=interval)

        if len(data) < 1:
            print(f"Failed to download data {tick_name}. Please check logs for details")
            return False
        else:
            logging.info(f"Downloaded {tick_name}")
    except Exception as e:
        logging.error(f"Could not download ticker with exception: {e}")
        return False

    # Write to database
    keys = list(data.index)
    db = pgh()

    # For futures remove the =F and insert _F for db name compatibility
    if '=F' in tick_name:
        tick_name = tick_name.replace('=', '')

    db.insert_stock_data(tick_name, interval, keys, data)

    return True


def insert_csv(tick_name, interval, file_name) -> bool:
    """ Used to insert CSV data into the database """
    data = pd.read_csv(file_name, index_col=0)

    # Write to database
    keys = list(data.index)
    db = pgh()

    # For futures remove the =F and insert _F for db name compatibility
    if '=F' in tick_name:
        tick_name = tick_name.replace('=', '')

    db.insert_stock_data(tick_name, interval, keys, data)

    return True
