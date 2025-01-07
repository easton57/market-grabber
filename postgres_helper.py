"""
Postgres helper for the automated market trader
Currently used in: Market Saver SL
Planned to be used in: Market Saver SL
"""

import logging
import keyring
import configparser as cp
import psycopg
import psycopg.errors
import datetime as dt

from getpass import getpass


class PsSQLHelper:
    def __init__(self):
        """ Initializer for PsSQLHelper class """
        # read from conf file, prompt if they don't exist. user, password (set in keyring), host, port
        config = cp.ConfigParser()
        config.read('conf/postgres_helper.conf')

        try:
            # Read the config
            user = config["connection"]["user"]
            password_set = config["connection"]["password_set"]
            host = config["connection"]["host"]
            port = int(config["connection"]["port"])

        except KeyError:
            user = input("Please enter your PostgreSQL username: ")
            host = input("Enter your PostgreSQL hostname: ")
            port = '5432'
            password_set = 'False'

            config["connection"] = {
                "user": user,
                "password_set": password_set,
                "host": host,
                "port": port
            }

        # Attempt to get the password and have it set if empty
        password = keyring.get_password('market_saver', user)

        if password is None or password_set == "False":
            password = getpass(f"Please enter your database password for {user}: ")
            keyring.set_password('market_saver', user, password)
            config["connection"]["password_set"] = 'True'

            # write the config
            with open('conf/postgres_helper.conf', 'w') as f:
                config.write(f)


        if user is None or password is None:
            logging.info("Username or password is empty! Those are required fields!")
            return

        # Attempt to connect to the database
        try:
            self.conn = psycopg.connect(
                host=host,
                dbname="market_saver_db",
                user=user,
                password=password,
                port=port,
                autocommit=True)

            logging.info("Connected successfully!")
        except psycopg.OperationalError:  # TODO: Check for and declare an error if the password is incorrect
            try:
                logging.warning("Database doesn't exist, attempting to create database before continuing...")

                # try to create the database
                conn = psycopg.connect(
                    host=host,
                    dbname="postgres",
                    user=user,
                    password=password,
                    port=port)

                # Create cursor and send the query
                cur = conn.cursor()
                cur.execute("CREATE DATABASE market_saver_db_sl;")
                conn.close()

                # create global connection
                self.conn = psycopg.connect(
                    host=host,
                    dbname="market_saver_db_sl",
                    user=user,
                    password=password,
                    port=port)

                logging.info("Connected successfully!")

            except Exception as e:
                logging.error(f"Could not create database! Manually create market_saver_db and try again.\n {e}")
                return
        except psycopg.DatabaseError:
            logging.error(f"Failed to connect: Unable to find database at address: {host}")
            return

        # Create our cursor
        self.cur = self.conn.cursor()

        # Create ticker table if it doesn't exist
        try:
            self.cur.execute("SELECT * FROM ticker")
        except psycopg.errors.UndefinedTable:
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
                 f"volume INT,"
                 f"action TEXT DEFAULT 'hold'"
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
        except psycopg.errors.UndefinedTable:
            self.conn.rollback()
            return False

        # The tick time span table
        try:
            self.cur.execute(f"SELECT * FROM {tick_name}_{time_span}")
            logging.info("Tick time span table exists")
        except psycopg.errors.UndefinedTable:
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
            logging.info(f"Write '{query}' to database")
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
                     f"{float(data.loc[keys[i], 'Open'])}, "
                     f"{float(data.loc[keys[i], 'High'])}, "
                     f"{float(data.loc[keys[i], 'Low'])}, "
                     f"{float(data.loc[keys[i], 'Close'])}, "
                     f"{int(data.loc[keys[i], 'Volume'])});")

            self.execute_query(query)

        # sort the data in the table
        query = (f"CLUSTER {tick_name}_{time_span} USING {tick_name}_{time_span}_idx;")

        self.execute_query(query)
