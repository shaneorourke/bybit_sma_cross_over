import pandas as pd
from pybit.usdt_perpetual import HTTP
import bybit_secrets as sc
import datetime as dt
import sqlite3 as sql
from time import sleep
import ta

from pytz import HOUR

conn = sql.connect('bybit_sma')
cur = conn.cursor()
cur.execute('CREATE TABLE IF NOT EXISTS Logs (id integer PRIMARY KEY AUTOINCREMENT, log_type text, order_id text, symbol text, close decimal, fast_sma decimal, slow_sma decimal, cross text, last_cross text, buy_sell text, market_date timestamp DEFAULT current_timestamp)')
cur.execute('INSERT OR REPLACE INTO Logs (id,log_type,order_id,symbol,close,fast_sma,slow_sma,cross,last_cross,buy_sell) VALUES (1,"log","na",NULL,0,0,0,"wait","na","na")')
conn.commit()

session = HTTP("https://api.bybit.com",
               api_key= sc.API_KEY, api_secret=sc.API_SECRET,request_timeout=30)
try:
    session.set_leverage(symbol="SOLUSDT",buy_leverage=1,sell_leverage=1)
except Exception as e:
    error = e

now_today = dt.datetime.now()
now_timestamp = dt.datetime.now()
now = now_today + dt.timedelta(days=-3)
today = dt.datetime(now.year, now.month, now.day)

def applytechnicals(df):
    df['FastSMA'] = df.close.rolling(7).mean()
    df['SlowSMA'] = df.close.rolling(25).mean()
    df['%K'] = ta.momentum.stoch(df.high,df.low,df.close,window=14,smooth_window=3)
    df['%D'] = df['%K'].rolling(3).mean()
    df['rsi'] = ta.momentum.rsi(df.close,window=14)
    df['macd'] = ta.trend.macd_diff(df.close)
    df.dropna(inplace=True)
    return df

def get_bybit_bars(trading_symbol, interval, startTime, apply_technicals):
    startTime = str(int(startTime.timestamp()))
    response = session.query_kline(symbol=trading_symbol,interval=interval,from_time=startTime)
    df = pd.DataFrame(response['result'])
    df.start_at = pd.to_datetime(df.start_at, unit='s') + pd.DateOffset(hours=1)
    df.open_time = pd.to_datetime(df.open_time, unit='s') + pd.DateOffset(hours=1)
    if apply_technicals:
        applytechnicals(df)
    return df

def insert_log(trading_symbol,type,order_id,close_price,fast_sma,slow_sma,cross,last_cross,buy_sell):
    if str(buy_sell).upper() not in ('LONG','SHORT'):
        buy_sell == None
    insert_query = f'INSERT INTO Logs (log_type,order_id,symbol,close,fast_sma,slow_sma,cross,last_cross,buy_sell) VALUES ("{trading_symbol}","{type}","{order_id}",{close_price},{fast_sma},{slow_sma},"{cross}","{last_cross}","{buy_sell}")'
    cur.execute(insert_query)
    conn.commit()

def read_last_log():
    query = 'SELECT id,log_type,order_id,symbol,close,fast_sma,slow_sma,cross,last_cross,market_date,buy_sell FROM logs ORDER BY id DESC LIMIT 1 '
    cur.execute(query)
    output = cur.fetchone()
    id = output[0]
    type = output[1]
    order_id = output[2]
    symbol = output[3]
    close = output[4]
    fast_sma = output[5]
    slow_sma = output[6]
    cross = output[7]
    last_cross = output[8]
    market_date = output[9]
    buy_sell = output[10]
    return id,type,order_id,symbol,close,fast_sma,slow_sma,cross,last_cross,market_date,buy_sell

def print_Last_log():
    log = read_last_log()
    print(f'{now_today}:id:{log[0]}')
    print(f'{now_today}:type:{log[1]}')
    print(f'{now_today}:order_id:{log[2]}')
    print(f'{now_today}:symbol:{log[3]}')
    print(f'{now_today}:close:{log[4]}')
    print(f'{now_today}:fast_sma:{log[5]}')
    print(f'{now_today}:slow_sma:{log[6]}')
    print(f'{now_today}:cross:{log[7]}')
    print(f'{now_today}:last_cross:{log[8]}')
    print(f'{now_today}:market_date:{log[9]}')
    print(f'{now_today}:buy_sell:{log[10]}')
    print()

def get_quantity(close_price):
    funds = pd.DataFrame(session.get_wallet_balance()['result'])
    funds.to_sql(con=conn,name='Funds',if_exists='replace')
    get_available_bal = 'select USDT from Funds where "index" = "available_balance"'
    cur.execute(get_available_bal)
    available_balance = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    qty = round((available_balance / close_price),1)
    qty = round(qty - 0.1,2)
    return qty

def sma_cross_detect(df:object):
    previous_candle = df.iloc[-2]
    current_candle = df.iloc[-1]
    if previous_candle.FastSMA > previous_candle.SlowSMA:
        previous_sma = 'up'
    elif previous_candle.FastSMA < previous_candle.SlowSMA:
        previous_sma = 'down'
    else:
        previous_sma = 'unknown'

    if current_candle.FastSMA > current_candle.SlowSMA:
        current_sma = 'up'
    elif current_candle.FastSMA < current_candle.SlowSMA:
        current_sma = 'down'
    else:
        current_sma = 'unknown'
    return previous_sma, current_sma

def sma_cross_entry_strategy(df:object,symbol:str):
    previous_sma = sma_cross_detect(df)[0]
    current_sma = sma_cross_detect(df)[1]
    qty = get_quantity(df.close.iloc[-1])
    current_price = df.close.iloc[-1]
    side = 'na'

    if previous_sma == 'up' and current_sma == 'down':
        print('OPEN SHORT')
        side = "Sell"
        order_id = place_order(symbol,side,qty,current_price)
        insert_log('order_open',order_id,trading_symbol,current_price,df.FastSMA.iloc[-1],df.SlowSMA.iloc[-1],current_sma,previous_sma,side)

    elif previous_sma == 'down' and current_sma == 'up':
        print('OPEN LONG')
        side = "Buy"
        order_id = place_order(symbol,side,qty,current_price)  
        insert_log('order_open',order_id,symbol,current_price,df.FastSMA.iloc[-1],df.SlowSMA.iloc[-1],current_sma,previous_sma,side)
     
    else:
        insert_log('log','na',symbol,current_price,df.FastSMA.iloc[-1],df.SlowSMA.iloc[-1],current_sma,previous_sma,side)

def sma_cross_exit_strategy(df:object,symbol:str):
    previous_sma = sma_cross_detect(df)[0]
    current_sma = sma_cross_detect(df)[1]
    order_id = get_last_order(trading_symbol)[0]
    current_price = df.close.iloc[-1]
    side = get_last_order_side(symbol)
    
    if side == 'Buy' and current_sma == 'down':
        print('CLOSE LONG')
        close_position(symbol)
        insert_log('order_close',order_id,symbol,current_price,df.FastSMA.iloc[-1],df.SlowSMA.iloc[-1],current_sma,previous_sma,side)

    elif side == 'Sell' and current_sma == 'up':
        print('CLOSE SHORT')
        close_position(symbol)
        insert_log('order_close',order_id,symbol,current_price,df.FastSMA.iloc[-1],df.SlowSMA.iloc[-1],current_sma,previous_sma,side)

    else:
        insert_log('log',order_id,symbol,current_price,df.FastSMA.iloc[-1],df.SlowSMA.iloc[-1],current_sma,previous_sma,"na")


def place_order(trading_symbol,order_side,quantity,buy_price):
    order_df = pd.DataFrame(session.place_active_order(symbol=trading_symbol,
                                        side=f"{order_side}",
                                        order_type="Market",
                                        qty=quantity,
                                        price=buy_price,
                                        time_in_force="ImmediateOrCancel",
                                        reduce_only=False,
                                        close_on_trigger=False)['result'],index=[0])
    return order_df.order_id.iloc[-1]


def get_last_order(trading_symbol):
    cur.execute(f'select order_id from Logs where symbol="{trading_symbol}" and log_type != "log" order by id desc')
    order_id = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    return order_id
    
def get_last_order_side(trading_symbol):
    cur.execute(f'select buy_sell from Logs where symbol="{trading_symbol}" and log_type != "log" order by id desc')
    buy_sell = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    return buy_sell

def close_position(trading_symbol):
    session.close_position(symbol=trading_symbol)
    conn.commit()

def check_open_position():
    position = pd.DataFrame(session.my_position(symbol=trading_symbol)['result'])
    position.to_sql(con=conn,name='Position',if_exists='replace')
    open_position = position[position.columns[0]].count()
    cur.execute(f'select sum(size) from Position')
    open_position = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    return open_position

if __name__ == '__main__':
    trading_symbol = "SOLUSDT"
    interval='60'
    trailing_stop_take_profit = True
    candles = get_bybit_bars(trading_symbol,interval,today,True)

    open_position = check_open_position()
    if not open_position > 0.0:
        sma_cross_entry_strategy(candles,trading_symbol)
    else:
        sma_cross_exit_strategy(candles,trading_symbol)

    cur.close()
    conn.close()
    conn = sql.connect('bybit_sma')
    cur = conn.cursor()

