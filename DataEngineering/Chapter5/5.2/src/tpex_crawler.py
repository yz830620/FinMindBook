import sys
import time

import pandas as pd
import requests
from loguru import logger
from .utils.sql_basemodel import TaiwanStockPrice
from .base_crawler import BaseCrawler


class TpexCrawler(BaseCrawler):
    """class to define the Tpex crawler"""

    def __init__(self):
        self.schema_type = TaiwanStockPrice

    def clear_data(self, df: pd.DataFrame,) -> pd.DataFrame:
        """資料清理, 將文字轉成數字"""
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
                .str.replace(" ", "")
                .str.replace("除權息", "0")
                .str.replace("除息", "0")
                .str.replace("除權", "0")
            )
        return df

    def colname_formation(self,
                          df: pd.DataFrame,
                          ) -> pd.DataFrame:
        """設定資料欄位名稱"""
        df.columns = [
            "StockID",
            "Close",
            "Change",
            "Open",
            "Max",
            "Min",
            "TradeVolume",
            "TradeValue",
            "Transaction",
        ]
        return df

    def convert_to_tw_date(self, date: str) -> str:
        year, month, day = date.split("-")
        year = int(year) - 1911
        return f"{year}/{month}/{day}"

    def crawler(self,
                date: str,
                ) -> pd.DataFrame:
        """
        櫃買中心網址
        https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php?l=zh-tw
        """
        # headers 中的 Request url
        url_base = 'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php?l=zh-tw'
        url = (url_base + "&d={date}&se=AL")
        url = url.format(
            date=self.convert_to_tw_date(date)
        )
        # 避免被櫃買中心 ban ip, 在每次爬蟲時, 先 sleep 5 秒
        time.sleep(5)
        # request method
        res = requests.get(
            url, headers=self.get_header(url_base)
        )
        data = res.json().get("aaData", "")
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)

        if len(df) == 0:
            return pd.DataFrame()
        # 櫃買中心回傳的資料, 並無資料欄位, 因此這裡直接用 index 取特定欄位
        df = df[[0, 2, 3, 4, 5, 6, 7, 8, 9]]
        # 欄位中英轉換
        df = self.colname_formation(df.copy())
        df["date"] = date
        return df


if __name__ == "__main__":
    tpex_crawler = TpexCrawler()
    start_date, end_date = sys.argv[1:]
    save_filename = "taiwan_stock_price_tpex"
    tpex_crawler.main(start_date, end_date, save_filename)
