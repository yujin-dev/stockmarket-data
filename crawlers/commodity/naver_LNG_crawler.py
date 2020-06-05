import requests as req
import pandas as pd
import numpy as np
import os
import datetime as dt
import sys
import io
import time
from multiprocessing import Pool
from html_table_parser import parser_functions as parser
from bs4 import BeautifulSoup as bs


def get_response(page):

    url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_NG&page={}".format(
        page
    )
    res = req.get(url)
    return res


def LNG_data_parsing():

    total_df = pd.DataFrame()
    for i in range(1, 472):
        res = get_response(i)
        parsed_html = parser.make2d(bs(res.content, "lxml").find("tbody"))
        df = pd.DataFrame(parsed_html).iloc[:, :2]
        df.set_index(0, inplace=True)
        df.columns = ["종가"]
        total_df = pd.concat([total_df, df])

    total_df.index.name = "date"
    total_df = total_df.astype(np.float32)
    total_df.index = total_df.index.map(lambda x: x.replace(".", "-"))
    total_df.to_csv("NG_naver_price.csv", encoding="cp949")
    return total_df


if __name__ == "__main__":

    LNG_data_parsing()
