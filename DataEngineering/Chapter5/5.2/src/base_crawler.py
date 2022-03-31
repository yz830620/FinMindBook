from abc import ABC, abstractmethod
from typing import Optional
import datetime
import typing

import pandas as pd
from loguru import logger
from pydantic import BaseModel


class BaseCrawler(ABC):
    """class for the base crawler"""

    def __init__(self):
        self.schema_type: Optional[BaseModel] = None

    def get_header(self, url):
        """網頁瀏覽時, 所帶的 request header 參數, 模仿瀏覽器發送 request"""
        return {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Host": "www.tpex.org.tw",
            "Referer": f"{url}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
        }

    def colname_formation(self):
        pass

    @abstractmethod
    def clean_data(self):
        raise NotImplementedError

    @abstractmethod
    def crawler(self):
        raise NotImplementedError

    def check_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """檢查資料型態, 確保每次要上傳資料庫前, 型態正確"""

        assert self.schema_type, "self.schema_type not None"

        df_dict = df.to_dict("records")
        df_schema = [
            self.schema_type(**dd).__dict__
            for dd in df_dict
        ]
        df = pd.DataFrame(df_schema)
        return df

    def gen_date_list(self, start_date: str, end_date: str) -> typing.List[str]:
        """建立時間列表, 用於爬取所有資料"""
        start_date = (
            datetime.datetime.strptime(
                start_date, "%Y-%m-%d"
            ).date()
        )
        end_date = (
            datetime.datetime.strptime(
                end_date, "%Y-%m-%d"
            ).date()
        )
        days = (
            end_date - start_date
        ).days + 1
        date_list = [
            str(
                start_date
                + datetime.timedelta(
                    days=day
                )
            )
            for day in range(days)
        ]
        return date_list

    def main(self, start_date: str, end_date: str, file_name: str):
        """櫃買中心寫明, 本資訊自民國96年7月起開始提供"""
        date_list = self.gen_date_list(
            start_date, end_date
        )
        for date in date_list:
            logger.info(date)
            df = self.crawler(date)
            if len(df) > 0:
                # 資料清理
                df = self.clear_data(df.copy())
                # 檢查資料型態
                df = self.check_schema(df.copy())
                # 這邊先暫時存成 file，下個章節將會上傳資料庫
                df.to_csv(
                    f"{file_name}_{date}.csv",
                    index=False,
                )
