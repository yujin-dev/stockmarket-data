import requests as req
from datetime import datetime
from html_table_parser import parser_functions as parser
from bs4 import BeautifulSoup as bs
import pandas as pd


def get_todayOpen(stock_code):

    """
    :param stock_code: A 제외 종목 코드( ex. 005930 )
    :return: 오늘의 시가( int )
    """

    def get_response():
        url = "https://finance.naver.com/item/sise_day.nhn?code={}&page=1".format(
            stock_code
        )
        res = req.get(url)
        return res

    today = datetime.today().strftime("%Y.%m.%d")

    res = get_response()
    try:
        today_open = list(
            filter(
                lambda x: today in x,
                parser.make2d(bs(res.content, "lxml").find("table")),
            )
        ).pop()[3]
        today_open = int(today_open.replace(",", ""))

        print(stock_code, today, today_open)  ##  종목코드, 날짜, 시가 체크

        return today_open

    except:
        print(stock_code, today + " No Open Price")  ## 오늘 시가( 가격 데이터 ) 없음


if __name__ == "__main__":

    get_todayOpen("005930")
