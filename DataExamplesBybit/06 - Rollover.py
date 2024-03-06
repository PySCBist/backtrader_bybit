import datetime as dt
import backtrader as bt
from backtrader_bybit import BybitStore
from ConfigBybit.Config import Config  # Configuration file
from Strategy import StrategyJustPrintsOHLCVAndState  # Trading System


def get_timeframe(tf, TimeFrame):
    """Converting TF to parameters for adding strategy data"""
    interval = 1  # by default, the timeframe is 1m
    _timeframe = TimeFrame.Minutes  # by default, the timeframe is 1m

    if tf == '1': interval = 1
    if tf == '5': interval = 5
    if tf == '15': interval = 15
    if tf == '30': interval = 30
    if tf == '60': interval = 60
    if tf == 'D': _timeframe = TimeFrame.Days
    if tf == 'W': _timeframe = TimeFrame.Weeks
    if tf == 'M': _timeframe = TimeFrame.Months
    return _timeframe, interval


# Gluing the ticker history from a file and Bybit (Rollover)
if __name__ == '__main__':  # Entry point when running this script
    cerebro = bt.Cerebro(quicknotify=True)

    coin_target = 'USDT'  # the base ticker in which calculations will be performed
    symbol = 'BTC' + coin_target  # the ticker by which we will receive data in the format <CodeTickerBaseTicker>

    accountType = Config.BYBIT_ACCOUNT_TYPE
    store = BybitStore(
        api_key=Config.BYBIT_API_KEY,
        api_secret=Config.BYBIT_API_SECRET,
        coin_target=coin_target,
        testnet=False,
        accountType=accountType,
    )  # Bybit Storage
    broker = store.getbroker()
    cerebro.setbroker(broker)

    tf = "D"  # '1m'  '5m' '15m' '30m' '1h' '1d' '1w' '1M'
    _t, _c = get_timeframe(tf, bt.TimeFrame)

    d1 = bt.feeds.GenericCSVData(  # We get the history from the file - which does not contain the last 5 days
        timeframe=_t, compression=_c,  # to be in the same TF as d2
        dataname=f'{symbol}_{tf}_minus_5_days.csv',  # File to import from Bybit. Created from example 02 - Symbol data to DF.py
        separator=',',  # Columns are separated by commas
        dtformat='%Y-%m-%d %H:%M:%S',  # dtformat='%Y-%m-%d %H:%M:%S',  # Date/time format YYYY-MM-DD HH:MM:SS // '%Y-%m-%d'
        openinterest=-1,  # There is no open interest in the file
        sessionend=dt.time(0, 0),  # For daily data and above, the end time of the session is substituted. To coincide with the story, you need to set the closing time to 00:00
    )

    from_date = dt.datetime.now() - dt.timedelta(days=15)  # we take data for the last 15 days
    d2 = store.getdata(timeframe=_t, compression=_c, dataname=symbol, start_date=from_date, LiveBars=False)  # Historical data for the smallest time interval

    cerebro.rolloverdata(d1, d2, name=symbol)  # Glued ticker

    cerebro.addstrategy(StrategyJustPrintsOHLCVAndState, coin_target=coin_target)  # Adding a trading system

    cerebro.run()  # Launching a trading system
    cerebro.plot()  # Draw a chart
