import datetime
import sys
import time
import typing

import pandas as pd
import requests
from loguru import logger
from .utils.sql_basemodel import TaiwanStockPrice
from .base_crawler import BaseCrawler


class TwseCrawler(BaseCrawler):
    """class to define the Twse crawler"""

    def __init__(self):
        self.schema_type = TaiwanStockPrice

    def clear_data(
        df: pd.DataFrame,
    ) -> pd.DataFrame:
        """資料清理, 將文字轉成數字"""
        df["Dir"] = (
            df["Dir"]
            .str.split(">")
            .str[1]
            .str.split("<")
            .str[0]
        )
        df["Change"] = (
            df["Dir"] + df["Change"]
        )
        df["Change"] = (
            df["Change"]
            .str.replace(" ", "")
            .str.replace("X", "")
            .astype(float)
        )
        df = df.fillna("")
        df = df.drop(["Dir"], axis=1)
        for col in [
            "TradeVolume",
            "Transaction",
            "TradeValue",
            "Open",
            "Max",
            "Min",
            "Close",
            "Change",
        ]:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "")
                .str.replace("X", "")
                .str.replace("+", "")
                .str.replace("----", "0")
                .str.replace("---", "0")
                .str.replace("--", "0")
            )
        return df

    def colname_formation(
        df: pd.DataFrame,
        colname: typing.List[str],
    ) -> pd.DataFrame:
        """資料欄位轉換, 英文有助於接下來存入資料庫"""
        taiwan_stock_price = {
            "證券代號": "StockID",
            "證券名稱": "",
            "成交股數": "TradeVolume",
            "成交筆數": "Transaction",
            "成交金額": "TradeValue",
            "開盤價": "Open",
            "最高價": "Max",
            "最低價": "Min",
            "收盤價": "Close",
            "漲跌(+/-)": "Dir",
            "漲跌價差": "Change",
            "最後揭示買價": "",
            "最後揭示買量": "",
            "最後揭示賣價": "",
            "最後揭示賣量": "",
            "本益比": "",
        }
        df.columns = [
            taiwan_stock_price[col]
            for col in colname
        ]
        df = df.drop([""], axis=1)
        return df

    def crawler(self,
                date: str,
                ) -> pd.DataFrame:
        """
        證交所網址
        https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html
        """
        # headers 中的 Request url
        url_base = "https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html"
        url = (
            "https://www.twse.com.tw/exchangeReport/MI_INDEX"
            "?response=json&date={date}&type=ALL"
        )
        url = url.format(
            date=date.replace("-", "")
        )
        # 避免被證交所 ban ip, 在每次爬蟲時, 先 sleep 5 秒
        time.sleep(5)
        # request method
        res = requests.get(
            url, headers=self.get_header(url_base)
        )
        if (
            res.json()["stat"]
            == "很抱歉，沒有符合條件的資料!"
        ):
            # 如果 date 是周末，會回傳很抱歉，沒有符合條件的資料!
            return pd.DataFrame()
        # 2009 年以後的資料, 股價在 response 中的 data9
        # 2009 年以後的資料, 股價在 response 中的 data8
        # 不同格式, 在證交所的資料中, 是很常見的,
        # 沒資料的情境也要考慮進去，例如現在週六沒有交易，但在 2007 年週六是有交易的
        try:
            if "data9" in res.json():
                df = pd.DataFrame(
                    res.json()["data9"]
                )
                colname = res.json()[
                    "fields9"
                ]
            elif "data8" in res.json():
                df = pd.DataFrame(
                    res.json()["data8"]
                )
                colname = res.json()[
                    "fields8"
                ]
            elif res.json()["stat"] in [
                "查詢日期小於93年2月11日，請重新查詢!",
                "很抱歉，沒有符合條件的資料!",
            ]:
                return pd.DataFrame()
        except Exception as e:
            logger.exception(
                f"error occur while doing crawler_twse, please check {url}")
            return pd.DataFrame()

        if len(df) == 0:
            return pd.DataFrame()
        # 欄位中英轉換
        df = self.colname_formation(
            df.copy(), colname
        )
        df["date"] = date
        return df


if __name__ == "__main__":
    twse_crawler = TwseCrawler()
    start_date, end_date = sys.argv[1:]
    save_filename = "taiwan_stock_price_twse"
    twse_crawler.main(start_date, end_date, save_filename)
