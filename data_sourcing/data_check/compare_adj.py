import pandas as pd
import numpy as np
from mysql.mysql_DB import MySql
from mysql.config import DBConfig
import sys
import requests
import json
import asyncio
import pickle
from daum_price import Daum_AdjPrice

class ComparePrice:

    def __init__(self, date, daum = None):

        self.date = date.replace('-', '')
        self.total_adj = daum

    def get_last_adjusted(self):

        sql_config = getattr(DBConfig, "financeteam_stockmarket")

        connect_sql = MySql(
            host=sql_config["host"],
            port=sql_config["port"],
            user=sql_config["user"],
            pwd=sql_config["password"],
            schema=sql_config["schema"],
        )

        query = "select STOCK_CODE, ADJ_CLOSE from hist_price where TRD_DD = {}".format(
            self.date
        )
        last_adj_close = connect_sql.read_sql(query)
        return last_adj_close


    def compare_adj_price(self):

        ''' 수정비율 변화 체크 -> 전날 소싱받은 수정주가 vs. 당일 받은 전날 수정주가 '''

        raw = self.get_last_adjusted()
        raw_stock_list = list(raw["STOCK_CODE"])
        raw.set_index("STOCK_CODE", inplace=True)
        
        if self.total_adj is None:
            price_crawler = Daum_AdjPrice(self.date, raw_stock_list)
            price_crawler.run()
            self.total_adj = price_crawler.total_adjusted

        adj_vs_close = []

        def compare(stock):
            
            if (
                self.total_adj[stock] == raw.loc[stock, 'ADJ_CLOSE']
            ) == False and (np.isnan(raw.loc[stock, 'ADJ_CLOSE']) == False):
                print(f"# {stock} # 전날 기준과 당일 기준 {self.date} 수정주가 다릅니다 : {self.total_adj[stock]} != { raw.loc[stock, 'ADJ_CLOSE']}")
                adj_vs_close.append(stock)

        list(map(compare, raw_stock_list))
        return adj_vs_close


if __name__ == "__main__":

    ComparePrice('20200604').compare_adj_price()