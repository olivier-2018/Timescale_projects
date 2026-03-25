import time
import sys
import psycopg2
import os
from dotenv import load_dotenv

from twelvedata import TDClient
from psycopg2.extras import execute_values
from datetime import datetime

load_dotenv()

class WebsocketPipeline():
    # name of the hypertable
    DB_TABLE = "stocks_real_time"

    # columns in the hypertable in the correct order
    DB_COLUMNS=["time", "symbol", "price", "day_volume"]

    # batch size used to insert data in batches
    MAX_BATCH_SIZE=100

    def __init__(self, conn):
        """Connect to the Twelve Data web socket server and stream
        data into the database.

        Args:
            conn: psycopg2 connection object
        """
        self.conn = conn
        self.current_batch = []
        self.insert_counter = 0

    def _insert_values(self, data):
        if self.conn is not None:
            cursor = self.conn.cursor()
            sql = f"""
            INSERT INTO {self.DB_TABLE} ({','.join(self.DB_COLUMNS)})
            VALUES %s;"""
            execute_values(cursor, sql, data)
            self.conn.commit()

    def _on_event(self, event):
        """This function gets called whenever there's a new data record coming
        back from the server.

        Args:
            event (dict): data record
        """
        if event["event"] == "price":
            # data record
            # timestamp = datetime.utcfromtimestamp(event["timestamp"])
            timestamp = datetime.fromtimestamp(event["timestamp"])
            
            data = (timestamp, event["symbol"], event["price"], event.get("day_volume"))

            # add new data record to batch
            self.current_batch.append(data)
            print(f"Current batch size: {len(self.current_batch)} - Data: {data}", flush=True)

            # ingest data if max batch size is reached then reset the batch
            if len(self.current_batch) == self.MAX_BATCH_SIZE:
                self._insert_values(self.current_batch)
                self.insert_counter += 1
                print(f"Batch insert #{self.insert_counter}\n", flush=True)
                self.current_batch = []
                
    def start(self, symbols):
        """Connect to the web socket server and start streaming real-time data
        into the database.

        Args:
            symbols (list of symbols): List of stock/crypto symbols
        """
        td = TDClient(apikey=os.getenv("TWELVEDATA_API_KEY"))
        ws = td.websocket(on_event=self._on_event)
        ws.subscribe(symbols)
        ws.connect()
        while True:
            ws.heartbeat()
            time.sleep(10)
              
try: 
    conn = psycopg2.connect(database=os.getenv("POSTGRES_DB") ,
                            host=os.getenv("POSTGRES_HOST") ,
                            user=os.getenv("POSTGRES_USER") ,
                            password=os.getenv("POSTGRES_PASSWORD") ,
                            port="5432")
    print(f"Connecting to database successful.")
    
except psycopg2.Error as e:
    print(f"Error connecting to database: {e}")
    conn = None
    sys.exit(1)
    
symbols = ["BTC/USD", "ETH/USD", "MSFT", "AAPL"]
websocket = WebsocketPipeline(conn)
websocket.start(symbols=symbols)
