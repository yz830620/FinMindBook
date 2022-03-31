import ABC
import datetime
import io
import sys
import time
import typing

import pandas as pd
import requests
from loguru import logger
from pydantic import BaseModel

class BaseCrawler:
    """class for the base crawler"""
    def __init__(self):
        pass

    def get_header(self):
        pass

    def colname_formation(self):
        pass

    def clean_data(self):
        pass

    def crawler(self):
        pass

    def check_schema(self):
        pass

    def gen_date_list(self):
        pass

    def main(self):
        pass