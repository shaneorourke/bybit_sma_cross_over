import pandas as pd
import sqlite3 as sql


conn = sql.connect('bybit_sma')
cur = conn.cursor()

last_log = pd.read_sql('''
        SELECT id,log_type,order_id,close,fast_sma,slow_sma,cross,last_cross,market_date,buy_sell 
        FROM Logs
        ORDER BY id DESC
        LIMIT 1
        ''',conn)

print(last_log)
print()

PandL = pd.read_sql('''
        SELECT l1.symbol , l1.buy_sell buy_sell ,l1.close as open_price, l2.close as close_price, l2.cross, l1.market_date, l2.log_type,
        case when l1.buy_sell = "Buy" then l2.close - l1.close when l1.buy_sell = "Sell" then l1.close - l2.close end as profit
        FROM Logs l1
        inner join Logs l2
            on l1.order_id = l2.order_id
                and l1.log_type = "order_open" 
                and l2.log_type like "order_close%"
        ''',conn)

print(PandL)
print()
print(f'Profit Total:{round(PandL.profit.sum(),3)}')
