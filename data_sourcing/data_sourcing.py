import eikon as ek
import pandas as pd
import numpy as np
from kiwoom_api import Kiwoom
from datetime import datetime
import sys
import time
from PyQt5.QtWidgets import QApplication
import os


class DailySourcing:

    """ 데이터 소싱받기 """

    def __init__(
        self,
        ksp,
        ksq,
        start=datetime.today().strftime("%Y%m%d"),
        end=datetime.today().strftime("%Y%m%d"),
    ):

        today = datetime.now().date()
        fx = lambda x: "0" + str(x) if x < 10 else str(x)

        self.today = "Update/{}".format(fx(today.month) + fx(today.day))
        if end != datetime.today().strftime("%Y%m%d"):
            self.today = "Update/{}".format(end[4:])

        os.makedirs(self.today, exist_ok=True)
        print(self.today + " is creating  #############################")

        self.tsTickers = sorted(map(lambda x: x.split("A")[1] + ".KS", ksp))
        tsKsq = sorted(map(lambda x: x.split("A")[1] + ".KQ", ksq))
        self.tsTickers.extend(tsKsq)
        kwTickers = sorted(ksp)
        kwTickers.extend(sorted(ksq))
        self.kwTickers = sorted(map(lambda x: x.split("A")[1], kwTickers))
        self.start = start
        self.end = end

    def thomson(self, tickers, fields, parameters=None):

        """tickers = 종목 코드에 대한 list
            fields = 불러오고자 하는 기능
            parameters = 주로 기간에 대한 파라미터"""
        print(
            "Thomson starts  #############################"
        )  ########## >> 텔레그램까지 자동화( eikon 실행은 작업 스케줄러)

        while True:
            try:
                if parameters == None:
                    data = ek.get_data(tickers, fields=fields)
                    data = pd.Series(data)[0]
                    dates = (
                        pd.Series(ek.get_data(tickers, fields=["TR.CLOSEPRICE.date"]))[
                            0
                        ]["Date"]
                        .unique()[0]
                        .split("T")[0]
                    )
                    data["date"] = [dates for _ in range(len(tickers))]
                    print(data)
                    if len(data["Close Price"].dropna()) != 0:
                        return data

                else:
                    data = ek.get_data(tickers, fields=fields, parameters=parameters)
                    data = pd.Series(data)[0]
                    data["date"] = sum(
                        [
                            list(
                                pd.date_range(
                                    start=parameters["SDate"],
                                    end=parameters["EDate"],
                                    freq="B",
                                )
                            )
                            for _ in range(len(tickers))
                        ],
                        [],
                    )

                    if len(data["Close Price"].dropna()) != 0:
                        return data

                time.sleep(3)
                print("Restart sourcing")

            except:
                pass

            # raise ValueError("thomson data sourcing ERROR")

    def thomson_sourcing(self):

        ek.set_app_key("9c4370471d3c41ea9a2a8911d842812e941e06df")
        tr_fields = [
            "TR.CLOSEPRICE",
            "TR.OPENPRICE(Adjusted=1)",
            "TR.CLOSEPRICE(Adjusted=1)",
            "TR.HIGHPRICE(Adjusted=1)",
            "TR.LOWPRICE(Adjusted=1)",
            "TR.IssueMarketCap",
        ]

        sec = 4
        interval = int(np.floor(len(self.tsTickers) / sec))

        tot_data = pd.DataFrame()
        for i in range(0, len(self.tsTickers), interval):

            tsData = self.thomson(
                self.tsTickers[i : i + interval], tr_fields
            )  # , parameters={'SDate':self.start, 'EDate':self.end})
            tot_data = pd.concat([tot_data, tsData], axis=0)
            time.sleep(3)

        tot_data.columns = [
            "Instrument",
            "종가",
            "수정시가",
            "수정주가",
            "수정고가",
            "수정저가",
            "시가총액",
            "date",
        ]
        tot_data.to_csv("{}/thomson.csv".format(self.today), encoding="cp949")
        self.organize_data("thomson", tot_data)

    def kiwoom_sourcing(
        self, func_names=["get_net_buy", "get_curr_data"], new_sourcing=True
    ):  # datetime.today().strftime("%Y%m%d")):
        """Kiwoom에서 소싱( 인스턴스 생성해서 받음 )"""

        print("Kiwoom starts #############################")

        app = QApplication(sys.argv)
        get_class = Kiwoom()

        for func in func_names:

            total_data = pd.DataFrame()
            if new_sourcing == False and func == "get_net_buy":
                total_data = pd.read_csv(
                    "{}/kiwoom_net_buy.csv".format(self.today), engine="python"
                )

            elif new_sourcing == False and func == "get_curr_data":
                total_data = pd.read_csv(
                    "{}/kiwoom_basic.csv".format(self.today), engine="python"
                )

            for stock in self.kwTickers:

                if func == "get_net_buy":
                    get_class.get_net_buy(stock, self.end)
                    total_data = pd.concat([total_data, get_class.res])

                    total_data[["개인순매매", "기관순매매", "등록외국인순매매"]].apply(lambda x: x * 100)
                    total_data.to_csv(
                        "{}/kiwoom_net_buy.csv".format(self.today), encoding="cp949"
                    )

                elif func == "get_curr_data":
                    get_class.get_curr_data(stock)
                    total_data = pd.concat([total_data, get_class.res])

                    total_data.to_csv(
                        "{}/kiwoom_basic.csv".format(self.today), encoding="cp949"
                    )

                print(stock + " is uploaded")

            self.organize_data("kiwoom", total_data)
        print("Done")
        sys.exit(app.exec_())

    def to_pkl(self, data, data_name):
        data.to_pickle(os.path.join(self.today, data_name + ".pickle"))

    def organize_data(self, source_name, data):

        if source_name == "thomson":

            new_code = data["Instrument"].map(lambda x: "A" + x.split(".")[0])
            data["Instrument"] = new_code
            colums_name = set(data.columns) - set(["Instrument", "date"])
            print(colums_name)
            for key in colums_name:
                print(key)
                p_data = data.pivot(index="date", columns="Instrument", values=key)
                self.to_pkl(p_data, key)

        elif source_name == "kiwoom":

            colums_name = set(data.columns) - set(["일자", "종목코드"])

            for key in colums_name:

                p_data = data.pivot(index="일자", columns="종목코드", values=key)
                p_data.index = p_data.index.map(
                    lambda x: str(x)[:4] + "-" + str(x)[4:6] + "-" + str(x)[6:]
                )
                self.to_pkl(p_data, key)


def thomson_timeseries(
    tickers,
    fields,
    corax,
    start_date=datetime.today().strftime("%Y%m%d"),
    end_date=datetime.today().strftime("%Y%m%d"),
):
    """ 톰슨 로이터 시계열 데이터 소싱 """
    ek.set_app_key("9c4370471d3c41ea9a2a8911d842812e941e06df")

    try:
        data = ek.get_timeseries(
            tickers,
            fields=fields,
            start_date=start_date,
            end_date=end_date,
            interval="daily",
            corax=corax,
        )
        print(data)
    except:
        raise ValueError("thomson data sourcing ERROR")
    return data
