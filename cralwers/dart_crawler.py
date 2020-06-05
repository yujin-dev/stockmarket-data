from cralwers import DisclosureManagement
import requests as req
import datetime as dt
import time
from functools import reduce
from html_table_parser import parser_functions as parser
from bs4 import BeautifulSoup as bs


class DartCrawler:
    def __init__(self, tickers):

        api_key = open("./api_key.txt", "rt").read()
        self.dict = {}
        self.dart_crawler = DisclosureManagement(api_key)
        self.tickers = tickers  ##  단축코드(6자리)

    def get_rcept(self, st_date, recent_date, pblntf_ty, page_no, page_count):

        """
        ### st_date부터 recent_date까지의 일자 중 가장 최근의 공시 문서 접수번호를 가져옴

        # params #
        st_date: YYYYmmdd
        recent_date: YYYY.mm 최근 공시일자(API에서 이런 분기보고서(2019.09) 식으로 제공)

        """

        rcept_dict = {}
        except_dict = {}
        today_ = dt.datetime.strftime(dt.datetime.today(), "%Y-%m-%d").replace(
            "-", ""
        )  # st_date부터 today_까지의 날짜 안에서 검색

        # for name, code in zip(ticker_df['name'], ticker_df['ticker']):
        for stock_code in self.tickers:
            # 다트 API를 통해 공시 자료 <접수 번호> 데이터 프레임 반환

            rcept_df = self.dart_crawler.push_rcept_no_df(
                stock_code, st_date, today_, pblntf_ty, page_no, page_count
            )

            if type(rcept_df) == str:  # 만약 반환값이 문자열이면 예외 리스트
                except_dict[stock_code] = rcept_df

            else:

                for num, report_nm in enumerate(
                    list(rcept_df["report_nm"])
                ):  # 만약 반환값이 데이터프레임이면...

                    if (recent_date in report_nm) and (
                        "정정" not in report_nm
                    ):  # 일자가 최신이고 정정보고서가 아닌 문서번호를 가져오는 단계

                        rcept_num = rcept_df["rcept_no"][num]
                        rcept_dict[stock_code] = rcept_num  # 이를 접수 번호 딕셔너리로 삽입

                        break

                time.sleep(5)

        # return rcept_dict
        return self.make_dict_fs_text(rcept_dict)

    def get_parsed_html(self, rcept_no):

        param_ls = ["rcpNo", "dcmNo", "eleld", "offset", "length", "dtd"]

        url = "http://dart.fss.or.kr/dsaf001/main.do?rcpNo={}".format(rcept_no)

        res = req.get(url)

        js_text = bs(res.content, "html.parser").text
        # ==================== HTML parsing을 통해 필요한 부분 가져오기====================
        body = js_text.split("재무제표")[1]
        body = body.split("cnt++")[0]

        st_idx = body.index(rcept_no)

        if ".xtd" in body:
            ed_idx = body.index(".xtd")
            last_str = ".xtd"
        else:
            ed_idx = body.index(".xsd")
            last_str = ".xsd"

        js_text_ls = body[st_idx:ed_idx].replace("'", "").replace(" ", "")
        js_text_ls = js_text_ls.split(",")
        # ================================================================================

        param_dict = dict(
            [
                (param_key, param_val)
                for param_key, param_val in zip(param_ls, js_text_ls)
            ]
        )
        param_dict["str"] = last_str

        url = "https://dart.fss.or.kr/report/viewer.do?rcpNo={rcpNo}&dcmNo={dcmNo}&eleId={eleld}&offset={offset}&length={length}&dtd={dtd}{str}&displayImage=show".format(
            **param_dict
        )

        res = req.get(url)
        parsed_html = bs(res.content, "html.parser")

        return parsed_html

    def get_fs_by_text_from_dart(self, stock_code, rcept_dict):

        rcept_no = rcept_dict[stock_code]

        all_sent = []
        parsed_html = self.get_parsed_html(rcept_no)

        for sent in parsed_html.findAll("table"):
            list_in_list = parser.make2d(sent)
            all_sent.append(list_in_list)

        return all_sent

    def make_dict_fs_text(self, rcept_dict):

        dict_ = {}

        for stock_code in rcept_dict.keys():
            data = self.get_fs_by_text_from_dart(stock_code, rcept_dict)
            dict_[stock_code] = data

            time.sleep(15)

        return dict_
