import pandas as pd
import sqlite3 as sql

tp = 24.294 + (24.294 * 0.05)

conn = sql.connect('bybit_sma')
cur = conn.cursor()
cur.execute('''
        UPDATE Logs
        set take_profit = 25.509
        where order_id = "d64dc678-e115-45db-bcf2-4e5dd5b57e39" and log_type != "log"
        ''')
conn.commit()
