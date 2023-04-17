import pandas as pd
import sqlite3 as sql


conn = sql.connect('bybit_sma')
cur = conn.cursor()
logs = pd.read_sql('''
        SELECT id,log_type,order_id,close,fast_sma,slow_sma,cross,last_cross,market_date,buy_sell 
        FROM Logs
        ORDER BY id DESC
        ''',conn)

print(logs)
