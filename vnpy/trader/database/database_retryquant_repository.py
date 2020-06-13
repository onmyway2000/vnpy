from datetime import datetime
from typing import List, Dict, Optional, Sequence

from retryquant_common.common.constant import Catalog, BarPeriod
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData

from .database import BaseDatabaseManager

from retryquant_common.feeder.unified import FeederServiceAdapter


class RetryquantRepository(BaseDatabaseManager):
    def __init__(self):
        self.__adapter = FeederServiceAdapter()
        self.__interval_to_period = {Interval.MINUTE: BarPeriod.Minute1, Interval.HOUR: BarPeriod.Hour,
                                     Interval.DAILY: BarPeriod.Day, Interval.WEEKLY: BarPeriod.Week}

    def __symbol_to_catalog(self, symbol, exchange):
        if exchange is Exchange.SZSE:
            if ('000001' < symbol < '002000') or ('002001' < symbol < '100000') or ('300001' < symbol < '310000'):
                return Catalog.Stock, symbol
            elif '399001' < symbol < '400000':
                return Catalog.Index, symbol
            else:
                raise Exception("Not supported symbol:{0}.{1}".format(symbol, exchange))
        elif exchange is Exchange.SSE:
            if ('600000' < symbol < '687999') or ('688001' < symbol < '699999'):
                return Catalog.Stock, symbol
            elif ('000001' < symbol < '001000') or ('999900' < symbol < '999999'):
                return Catalog.Index, symbol
            else:
                raise Exception("Not supported symbol:{0}.{1}".format(symbol, exchange))
        else:
            raise Exception("Not supported symbol:{0}.{1}".format(symbol, exchange))

    def __bar_df_to_object(self, df, symbol, exchange):
        result = []
        for idx, row in df.iterrows():
            obj = BarData(str(exchange), symbol, exchange, idx)
            obj.interval = Interval.MINUTE
            obj.volume = row['volume']
            obj.open_interest = 0
            obj.open_price = row['open']
            obj.high_price = row['high']
            obj.low_price = row['low']
            obj.close_price = row['close']
            result.append(obj)
        return result

    def __minute_df_to_object(self, df, symbol, exchange):
        result = []
        for idx, row in df.iterrows():
            obj = BarData(str(exchange), symbol, exchange, idx)
            obj.interval = Interval.MINUTE
            obj.volume = row['volume']
            obj.open_interest = 0
            obj.open_price = row['price']
            obj.high_price = row['price']
            obj.low_price = row['price']
            obj.close_price = row['price']
            result.append(obj)
        return result

    def load_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval", start: datetime, end: datetime) -> \
            Sequence["BarData"]:
        catalog, code = self.__symbol_to_catalog(symbol, exchange)
        period = self.__interval_to_period[interval]
        if catalog == Catalog.Index and period == BarPeriod.Minute1:
            df = self.__adapter.get_minute_time_data(catalog, code, str(start), str(end))
            return self.__minute_df_to_object(df, symbol, exchange)
        else:
            df = self.__adapter.get_bar_data(period, catalog, code, str(start), str(end))
            return self.__bar_df_to_object(df, symbol, exchange)

    def load_tick_data(self, symbol: str, exchange: "Exchange", start: datetime, end: datetime) -> Sequence["TickData"]:
        raise Exception("Not implemented")

    def save_bar_data(self, datas: Sequence["BarData"]):
        raise Exception("This operation not supported on retryquant repository")

    def save_tick_data(self, datas: Sequence["TickData"]):
        raise Exception("This operation not supported on retryquant repository")

    def get_newest_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval") -> Optional["BarData"]:
        raise Exception("Not implemented")

    def get_oldest_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval") -> Optional["BarData"]:
        raise Exception("Not implemented")

    def get_newest_tick_data(self, symbol: str, exchange: "Exchange") -> Optional["TickData"]:
        raise Exception("Not implemented")

    def get_bar_data_statistics(self, symbol: str, exchange: "Exchange") -> List[Dict]:
        raise Exception("Not implemented")

    def delete_bar_data(self, symbol: str, exchange: "Exchange", interval: "Interval") -> int:
        raise Exception("This operation not supported on retryquant repository")

    def clean(self, symbol: str):
        pass
