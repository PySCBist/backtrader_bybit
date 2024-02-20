import pandas as pd
from collections import deque
from datetime import datetime, timedelta, date
from time import sleep

from backtrader.feed import DataBase
from backtrader.utils import date2num

from backtrader import TimeFrame as tf


def interval_to_milliseconds(interval: str):
    """Convert a Bybit interval string to milliseconds

    :param interval: Bybit interval string, e.g.: 1, 5, 15, 30, 60, 120, 240, D, W

    :return:
         int value of interval in milliseconds
         None if interval prefix is not a decimal integer
         None if interval suffix is not one of D, W

    """
    seconds_per_unit = {
        "1": 60,
        "5": 5 * 60,
        "15": 15 * 60,
        "30": 30 * 60,
        "60": 60 * 60,
        "120": 120 * 60,
        "240": 240 * 60,
        "D": 24 * 60 * 60,
        "W": 7 * 24 * 60 * 60,
    }
    try:
        return seconds_per_unit[interval] * 1000
    except (ValueError, KeyError):
        return None


class BybitData(DataBase):
    """Class for getting historical and live ticker data"""
    params = (
        ('drop_newest', False),
    )

    # States for the Finite State Machine in _load
    _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(3)

    def __init__(self, store, **kwargs):  # def __init__(self, store, timeframe, compression, start_date, LiveBars):
        """Initialization of required variables"""
        self.timeframe = tf.Minutes
        self.compression = 1
        self.start_date = None
        self.LiveBars = None
        self.is_live = False

        self._state = None

        self.symbol = self.p.dataname

        if hasattr(self.p, 'timeframe'): self.timeframe = self.p.timeframe
        if hasattr(self.p, 'compression'): self.compression = self.p.compression
        if 'start_date' in kwargs: self.start_date = kwargs['start_date']
        if 'LiveBars' in kwargs: self.LiveBars = kwargs['LiveBars']

        self._store = store
        self._data = deque()

        self.all_history_data = None  # all history by ticker
        self.all_ohlc_data = []  # all history by ticker
        # print("Ok", self.timeframe, self.compression, self.start_date, self._store, self.LiveBars, self.symbol)

    def _load(self):
        """Download method"""
        if self._state == self._ST_OVER:
            return False
        elif self._state == self._ST_LIVE:
            return self._load_kline()
        elif self._state == self._ST_HISTORBACK:
            if self._load_kline():
                return True
            else:
                self._start_live()

    def _load_kline(self):
        """Processing a single row of data"""
        try:
            kline = self._data.popleft()
        except IndexError:
            return None

        if type(kline) == list:
            timestamp, open_, high, low, close, volume, turnover = kline
            self.lines.datetime[0] = date2num(datetime.fromtimestamp(int(timestamp) / 1000))
            self.lines.open[0] = float(open_)
            self.lines.high[0] = float(high)
            self.lines.low[0] = float(low)
            self.lines.close[0] = float(close)
            self.lines.volume[0] = float(volume)

        return True

    def _handle_kline_socket_message(self, msg):
        if msg['topic'] == f'kline.{self.interval}.{self.symbol}':
            if msg['data'][0]['confirm']:  # Is closed
                kline = self._parser_to_kline(msg['data'][0]['start'], msg['data'][0])
                self._data.extend(kline.values.tolist())
        else:
            raise msg

    def _parser_dataframe(self, data):
        df = data.copy()
        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        df['timestamp'] = df['timestamp'].values.astype(float)
        df['open'] = df['open'].values.astype(float)
        df['high'] = df['high'].values.astype(float)
        df['low'] = df['low'].values.astype(float)
        df['close'] = df['close'].values.astype(float)
        df['volume'] = df['volume'].values.astype(float)
        df['turnover'] = df['turnover'].values.astype(float)
        return df

    def _parser_to_kline(self, timestamp, kline):
        df = pd.DataFrame([[timestamp, kline['open'], kline['high'],
                            kline['low'], kline['close'], kline['volume'], kline['turnover']]])
        return self._parser_dataframe(df)

    def _start_live(self):
        # if live mode
        if self.LiveBars:
            self._state = self._ST_LIVE
            self.is_live = True
            self.put_notification(self.LIVE)

            print(f"Live started for ticker: {self.symbol}")
            if self._store.category == 'linear':
                self._store.bybit_linear_socket.kline_stream(interval=int(self.interval), symbol=self.symbol,
                                                             callback=self._handle_kline_socket_message)
        else:
            self._state = self._ST_OVER

    def haslivedata(self):
        return self._state == self._ST_LIVE and self._data

    def islive(self):
        return True

    def start(self):
        """Getting historical data"""
        DataBase.start(self)

        # if the TF is not set correctly, then we do nothing
        self.interval = self._store.get_interval(self.timeframe, self.compression)
        if self.interval is None:
            self._state = self._ST_OVER
            self.put_notification(self.NOTSUPPORTED_TF)
            return

        # if we can't get the ticker data, then we don't do anything
        self.symbol_info = self._store.get_symbol_info(self.symbol)
        if self.symbol_info is None:
            self._state = self._ST_OVER
            self.put_notification(self.NOTSUBSCRIBED)
            return

        # getting historical data
        if self.start_date:
            self._state = self._ST_HISTORBACK
            self.put_notification(self.DELAYED)

            klines = self._get_historical_klines()

            self.get_live_bars_from = datetime.now().replace(second=0, microsecond=0)

            print(f"- {self.symbol} - History data - Ok")

            if 'result' in klines and 'list' in klines['result'] and klines['result']['list']:
                klines = klines['result']['list']
                klines = klines[::-1]  # inverse
                klines = klines[:-1]  # -1 last row as it can be in process of forming
            else:
                klines = []

            self.all_history_data = klines  # first receive of the history -> save it to a list

            try:
                if self.p.drop_newest:
                    klines.pop()
                self._data.extend(klines)
            except Exception as e:
                print("Exception (try set from_date in utc format):", e)

        else:
            self._start_live()

    def _get_historical_klines(self):
        output_data = []
        limit = 1000

        from_ts = self.start_date.timestamp() * 1000
        from_ts = int(max(self._get_earliest_valid_timestamp(), from_ts))

        end_ts = int(datetime.now().timestamp() * 1000)
        timeframe = interval_to_milliseconds(self.interval)

        idx = 0
        while True:

            temp_data = self._store.bybit_session.get_kline(category="linear",
                                                            symbol=self.symbol,
                                                            interval=self.interval,
                                                            start=from_ts,
                                                            end=end_ts,
                                                            limit=limit)

            start_data_timestamp = float(temp_data['result']['list'][-1][0]) / 1000

            if temp_data['result']['list']:
                if not output_data:
                    output_data = temp_data
                else:
                    output_data['result']['list'].extend(temp_data['result']['list'])

            end_ts = int(start_data_timestamp * 1000 - timeframe)

            if end_ts and end_ts <= from_ts:
                break

            if not len(temp_data) or len(temp_data['result']['list']) < limit:
                break

            idx += 1

            if idx % 3 == 0:
                sleep(1)

        return output_data

    def _get_earliest_valid_timestamp(self) -> float:
        info = self._store.bybit_session.get_instruments_info(category=self._store.category, symbol=self.symbol)
        return float(info['result']['list'][0]['launchTime'])
