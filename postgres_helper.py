"""
Postgres helper for the automated market trader
Currently used in: Market Saver SL
Planned to be used in: Market Saver SL
"""

import logging
import psycopg2
import psycopg2.errors
import datetime as dt
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class PsSQLHelper:
    def __init__(self, user, password, host='192.168.73.233', port=5432):
        """ Initializer for PsSQLHelper class """
        if user is None or password is None:
            logging.info("Username or password is empty! Those are required fields!")
            return

        # Attempt to connect to the database
        try:
            self.conn = psycopg2.connect(
                host=host,
                dbname="market_saver_db_rl",
                user=user,
                password=password,
                port=port)

            logging.info("Connected successfully!")
        except psycopg2.OperationalError:
            try:
                logging.warning("Database doesn't exist, attempting to create database before continuing...")

                # try to create the database
                conn = psycopg2.connect(
                    host=host,
                    dbname="postgres",
                    user=user,
                    password=password,
                    port=port)

                # Create cursor and send the query
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                cur.execute("CREATE DATABASE market_saver_db_rl;")
                conn.close()

                # create global connection
                self.conn = psycopg2.connect(
                    host=host,
                    dbname="market_saver_db_rl",
                    user=user,
                    password=password,
                    port=port)

                logging.info("Connected successfully!")

            except Exception as e:
                logging.error(f"Could not create database! Manually create market_saver_db and try again.\n {e}")
                return
        except psycopg2.DatabaseError:
            logging.error(f"Failed to connect: Unable to find database at address: {host}")
            return

        # Create our cursor
        self.cur = self.conn.cursor()

        # Create ticker table if it doesn't exist
        try:
            self.cur.execute("SELECT * FROM ticker")
        except psycopg2.errors.UndefinedTable:
            self.conn.rollback()
            self.create_ticker_table()

    def create_ticker_table(self) -> bool:
        """ Needed to verify that the primary table is available """
        # Create the query
        query = (f"CREATE TABLE ticker ("
                 f"tick_name VARCHAR(5),"
                 f"tick_type VARCHAR(7)"
                 f");")

        # Execute query and return status
        return self.execute_query(query)

    def create_tick_table(self, tick_name) -> bool:
        """ Create additional tables when a new ticker is added """
        query = (f"CREATE TABLE {tick_name} ("
                 f"time_span VARCHAR(3) PRIMARY KEY,"
                 f"downloaded_on TIMESTAMP"
                 f");")

        return self.execute_query(query)

    def create_tick_span_table(self, tick_name, time_span) -> bool:
        """ Used to create the table specific to a time span of an existing ticker """
        query = (f"CREATE TABLE {tick_name}_{time_span} ("
                 f"tick_time TIMESTAMP PRIMARY KEY,"
                 f"open_val FLOAT8,"
                 f"high_val FLOAT8,"
                 f"low_val FLOAT8,"
                 f"close_val FLOAT8,"
                 f"volume INT"
                 f"); ")

        # Send the query and return status of execution
        return self.execute_query(query)

    def create_idx(self, tick_name, time_span) -> bool:
        """ Create an index for sorting later """
        query = (f"CREATE INDEX {tick_name}_{time_span}_idx ON {tick_name}_{time_span} (tick_time);")

        # Send the query and return status of execution
        return self.execute_query(query)

    def table_exists(self, tick_name, time_span) -> bool:
        """ Check to see if the tables exist before placing data in them """
        # The ticker name table
        try:
            self.cur.execute(f"SELECT * FROM {tick_name}")
            logging.info("Tick name table exists")
        except psycopg2.errors.UndefinedTable:
            self.conn.rollback()
            return False

        # The tick time span table
        try:
            self.cur.execute(f"SELECT * FROM {tick_name}_{time_span}")
            logging.info("Tick time span table exists")
        except psycopg2.errors.UndefinedTable:
            self.conn.rollback()
            return False

        # Both are good, return true
        return True

    def execute_query(self, query) -> bool:
        """ Little function to clean up some redundant code """
        try:
            # Execute the query
            self.cur.execute(query)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error executing query: \n"
                          f"{query}\n\n"
                          f"With Error:\n"
                          f"{e}\n")
            return False

    def get_data(self, query) -> list:
        """ Little function to clean up some redundant code """
        try:
            # Execute the query
            self.cur.execute(query)
            self.conn.commit()
            rows = self.cur.fetchall()
            return rows
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error executing query: \n"
                          f"{query}\n\n"
                          f"With Error:\n"
                          f"{e}\n")
            return None

    def insert_stock_data(self, tick_name, time_span, keys, data) -> None:
        """ Insert stock data into the database from yfinance """
        # Make sure our tables exist
        table_exists = self.table_exists(tick_name, time_span)

        if not table_exists:
            self.create_tick_table(tick_name)
            self.create_tick_span_table(tick_name, time_span)
            self.create_idx(tick_name, time_span)

        # Add ticker to the ticker table if it doesn't exist
        query = (f"SELECT * FROM ticker "
                 f"WHERE tick_name = '{tick_name}'")

        tickers = self.get_data(query)

        if len(tickers) == 0:
            if tick_name[-1].lower() == 'F':
                tick_type = "Future"
            else:
                tick_type = "Stock"

            query = (f"INSERT INTO ticker(tick_name, tick_type) "
                     f"VALUES ("
                     f"'{tick_name}', "
                     f"'{tick_type}');")

            if not self.execute_query(query):
                return

        time = dt.datetime.now(dt.timezone.utc)

        # Add time_span and downloaded date to tick table
        query = (f"INSERT INTO {tick_name}(time_span, downloaded_on) "
                 f"VALUES ("
                 f"'{time_span}', "
                 f"'{time}');")

        # Update instead if there's an issue
        if not self.execute_query(query):
            query = (f"UPDATE {tick_name} "
                     f"SET downloaded_on = '{time}' "
                     f"WHERE time_span = '{time_span}'")

            self.execute_query(query)

        # Add data to tick_span_table
        for i in range(len(data)):
            query = (f"INSERT INTO {tick_name}_{time_span}(tick_time, open_val, high_val, low_val, close_val, volume) "
                     f"Values("
                     f"'{keys[i]}', "
                     f"{data.loc[keys[i], 'Open']}, "
                     f"{data.loc[keys[i], 'High']}, "
                     f"{data.loc[keys[i], 'Low']}, "
                     f"{data.loc[keys[i], 'Close']}, "
                     f"{data.loc[keys[i], 'Volume']});")

            self.execute_query(query)

        # sort the data in the table
        query = (f"CLUSTER {tick_name}_{time_span} USING {tick_name}_{time_span}_idx;")

        self.execute_query(query)
