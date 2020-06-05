import requests as req
import asyncio
from datetime import datetime
from html_table_parser import parser_functions as parser
from bs4 import BeautifulSoup as bs
import pandas as pd

class GetVol:

    def __init__(self, stock_list):

        self.stock_list = stock_list
        self.total_vol = {}

    async def current_vol(self, stock_code):

        """
        :param stock_code: A 제외 종목 코드( ex. 005930 )
        :return: 최신 거래량( int )
        """

        url = "https://finance.naver.com/item/sise_day.nhn?code={}&page=1".format(
            stock_code
        )
        res = await self.loop.run_in_executor(None, req.get, url)

        today = datetime.today().strftime("%Y.%m.%d")

        def parsing():

            try:
                volume = list(
                    filter(
                        lambda x: today in x,
                        parser.make2d(bs(res.content, "lxml").find("table")),
                    )
                ).pop()[6]
                volume = int(volume.replace(",", ""))
                self.total_vol[stock_code]=volume

                return 

            except:
                pass

        return await self.loop.run_in_executor(None, parsing)


    async def main(self):

        fts=  [asyncio.ensure_future(self.current_vol(stock)) for stock in self.stock_list]
        return await asyncio.gather(*fts)


    def run(self):

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.main())
        self.loop.close

