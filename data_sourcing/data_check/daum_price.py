import pandas as pd
import numpy as np
from mysql.mysql_DB import MySql
from mysql.config import DBConfig
import sys
import requests
import json
import asyncio
import pickle


class Daum_AdjPrice:
    
    def __init__(self, date, stock_list):
        
        self.stock_list = stock_list
        self.check_date  = date.replace('-', '')
        self.total_close = {}
        self.total_adjusted = {}


    def get_data(self, url, headers):

        data = requests.get(url, headers=headers)
       
        data = json.loads(data.text)
        data = pd.DataFrame(data["data"])[["candleTime", "tradePrice"]]
        data.set_index("candleTime", inplace=True)
        data.index = list(
            map(lambda x: x.split(" ")[0].replace("-", ""), data.index)
        )
        return data.loc[self.check_date, "tradePrice"]


    async def get_close(self, stock):

        url = "http://finance.daum.net/api/charts/%s/days?limit=%d&adjusted=false" % (
            stock,
            2,
        )

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Cookie": "GS_font_Name_no=0; GS_font_size=16; _ga=GA1.3.937989519.1493034297; webid=bb619e03ecbf4672b8d38a3fcedc3f8c; _ga=GA1.2.937989519.1493034297; _gid=GA1.2.215330840.1541556419; KAKAO_STOCK_RECENT=[%22A069500%22]; recentMenus=[{%22destination%22:%22chart%22%2C%22title%22:%22%EC%B0%A8%ED%8A%B8%22}%2C{%22destination%22:%22current%22%2C%22title%22:%22%ED%98%84%EC%9E%AC%EA%B0%80%22}]; TIARA=C-Tax5zAJ3L1CwQFDxYNxe-9yt4xuvAcw3IjfDg6hlCbJ_KXLZZhwEPhrMuSc5Rv1oty5obaYZzBQS5Du9ne5x7XZds-vHVF; webid_sync=1541565778037; _gat_gtag_UA_128578811_1=1; _dfs=VFlXMkVwUGJENlVvc1B3V2NaV1pFdHhpNTVZdnRZTWFZQWZwTzBPYWRxMFNVL3VrODRLY1VlbXI0dHhBZlJzcE03SS9Vblh0U2p2L2V2b3hQbU5mNlE9PS0tcGI2aXQrZ21qY0hFbzJ0S1hkaEhrZz09--6eba3111e6ac36d893bbc58439d2a3e0304c7cf3",
            "Host": "finance.daum.net",
            "If-None-Match": 'W/"23501689faaaf24452ece4a039a904fd"',
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
        }

        headers["Referer"] = "http://finance.daum.net/quotes/A%s" % stock

        res = await self.loop.run_in_executor(None, self.get_data, url, headers)

        return self.total_close.update({stock: res})


    async def get_adjusted(self, stock):

        url = (
            "http://finance.daum.net/api/charts/%s/days?limit=%d&adjusted=true"
            % (stock, 2)
        )

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
            "Cookie": "GS_font_Name_no=0; GS_font_size=16; _ga=GA1.3.937989519.1493034297; webid=bb619e03ecbf4672b8d38a3fcedc3f8c; _ga=GA1.2.937989519.1493034297; _gid=GA1.2.215330840.1541556419; KAKAO_STOCK_RECENT=[%22A069500%22]; recentMenus=[{%22destination%22:%22chart%22%2C%22title%22:%22%EC%B0%A8%ED%8A%B8%22}%2C{%22destination%22:%22current%22%2C%22title%22:%22%ED%98%84%EC%9E%AC%EA%B0%80%22}]; TIARA=C-Tax5zAJ3L1CwQFDxYNxe-9yt4xuvAcw3IjfDg6hlCbJ_KXLZZhwEPhrMuSc5Rv1oty5obaYZzBQS5Du9ne5x7XZds-vHVF; webid_sync=1541565778037; _gat_gtag_UA_128578811_1=1; _dfs=VFlXMkVwUGJENlVvc1B3V2NaV1pFdHhpNTVZdnRZTWFZQWZwTzBPYWRxMFNVL3VrODRLY1VlbXI0dHhBZlJzcE03SS9Vblh0U2p2L2V2b3hQbU5mNlE9PS0tcGI2aXQrZ21qY0hFbzJ0S1hkaEhrZz09--6eba3111e6ac36d893bbc58439d2a3e0304c7cf3",
            "Host": "finance.daum.net",
            "If-None-Match": 'W/"23501689faaaf24452ece4a039a904fd"',
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
        }

        headers["Referer"] = "http://fin(ance.daum.net/quotes/A%s" % stock

        res = await self.loop.run_in_executor(None, self.get_data, url, headers)

        return self.total_adjusted.update({stock:res})
    

    async def main_sourcing(self, price_type = 'adj'):

        if price_type =='close': 
            fts = [asyncio.ensure_future(self.get_close(stock)) for stock in self.stock_list]
        else:
            fts = [asyncio.ensure_future(self.get_adjusted(stock)) for stock in self.stock_list]

        return await asyncio.gather(*fts)


    def run(self, price_type = 'adj'):

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.main_sourcing(price_type))
        self.loop.close

