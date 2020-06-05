from collections import deque, defaultdict
from datetime import datetime
import sys
import time
import pandas as pd
import numpy as np

from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from PyQt5.QtCore import QObject
from PyQt5.QtWidgets import QApplication


class Kiwoom(QAxWidget):
    def __init__(self):

        super().__init__()

        self.delay_check = KiwoomAPIDelayCheck()

        self.kiwoom = self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        self.OnEventConnect.connect(self.getConnectState)
        self.OnReceiveTrData.connect(self.getCommData)
        self.getLogin()

        self.res = None

    def getLogin(self):

        self.dynamicCall("CommConnect()")

        # login Loop로 로그인 성공하면 loop을 빠져나감
        self.loginLoop = QEventLoop()
        self.loginLoop.exec_()

    def getConnectState(self):
        """
        현재 접속상태
        state: 0(미연결), 1(연결)
        """
        state = self.dynamicCall("GetConnectState()")

        if state == 1:
            print("Login 성공")
            self.loginLoop.exit()
        return state

    def getRepeatCnt(self, tr_code, record_name):

        repeat_num = self.dynamicCall(
            "GetRepeatCnt(QString, QString)", tr_code, record_name
        )
        return repeat_num

    def getLoginInfo(self):
        return_val = self.dynamicCall('GetLoginInfo("{}")'.format("USER_NAME"))
        return return_val

    def get_marketlist(self, market_num):
        """
        :param market_num
        '0'은 코스피, '10'은 코스닥
        """
        request_msg = 'GetCodeListByMarket("{}")'.format(market_num)

        if market_num == "0":
            self.kospi_listing = self.dynamicCall(request_msg)
            self.kospi_listing = self.kospi_listing.split(";")
        elif market_num == "10":
            self.kosdaq_listing = self.dynamicCall(request_msg)
            self.kosdaq_listing = self.kosdaq_listing.split(";")

    def code_korean(self):
        """
        :param code: 상장주식 종목 코드
        """
        for ind_code in self.kospi_listing:
            request_msg = 'GetMasterCodeName("{}")'.format(ind_code)
            ind_name = self.dynamicCall(request_msg)

    def getCommRqData(self, requestName, trCode, inquiry, screenNum):

        returnCode = self.dynamicCall(
            "CommRqData(QString, QString, int, QString)",
            requestName,
            trCode,
            inquiry,
            screenNum,
        )
        self.tr_request_Loop = QEventLoop()
        self.tr_request_Loop.exec_()

        return returnCode

    def setInputValue(self, key, value):

        self.dynamicCall("SetInputValue(QString, QString)", key, value)

    def get_stock_data(self, stock, input_param, rq_name, rq_code, output_param):

        for input_key, input_val in input_param.items():
            self.setInputValue(input_key, input_val)

        time.sleep(1)
        self.setInputValue("종목코드", stock)
        self.getCommRqData(rq_name, rq_code, 0, "0122")

        self.delay_check.checkDelay()

    def get_net_buy(self, stock, date=datetime.today().strftime("%Y%m%d")):

        """유형별 순매수대금"""

        self.stock = stock
        time.sleep(1)
        self.setInputValue("일자", date)
        self.setInputValue("종목코드", stock)
        self.setInputValue("금액수량구분", 1)
        self.setInputValue("매매구분", 0)

        self.getCommRqData("종목별투자자기관별요청", "opt10059", 0, "0122")
        # else:
        #    self.getCommRqData("종목별투자자기관별요청", "opt10059",2, "0122")
        self.delay_check.checkDelay()

    def get_curr_data(self, stock):

        """가장 최근 날짜의 기본정보( 종가 / 상장주식수 / 시가총액 등 )"""

        self.stock = stock
        time.sleep(1)
        self.setInputValue("종목코드", stock)
        self.getCommRqData("주식기본정보요청", "opt10001", 0, "0122")

        self.delay_check.checkDelay()

    def getCommData(
        self,
        screenNo,
        requestName,
        trCode,
        recordName,
        inquiry,
        deprecated1,
        deprecated2,
        deprecated3,
        deprecated4,
    ):
        collect_data = defaultdict(lambda: [])

        if requestName == "종목별투자자기관별요청":

            cnt = 1  # self.getRepeatCnt(trCode, requestName)

            for i in range(cnt):
                for key in ["일자", "개인투자자", "외국인투자자", "기관계"]:
                    data = self.dynamicCall(
                        "GetCommData(QString, QString, int, QString)",
                        trCode,
                        requestName,
                        i,
                        key,
                    )
                    collect_data[key].append(data.strip())
            collect_data = pd.DataFrame(collect_data)
            collect_data.rename(
                columns={"개인투자자": "개인순매매", "기관계": "기관순매매", "외국인투자자": "등록외국인순매매"},
                inplace=True,
            )
            collect_data["종목코드"] = [
                "A" + str(self.stock) for _ in range(len(collect_data))
            ]
            self.res = collect_data  # .replace('', np.nan).astype(np.float)

        elif requestName == "주식기본정보요청":

            for key in ["상장주식", "거래량", "시가총액"]:
                data = self.dynamicCall(
                    "GetCommData(QString, QString, int, QString)",
                    trCode,
                    requestName,
                    0,
                    key,
                )
                collect_data[key].append(data.strip())
            collect_data = pd.DataFrame(collect_data)
            collect_data.rename(columns={"상장주식": "상장주식수"}, inplace=True)
            collect_data["종목코드"] = "A" + str(self.stock)
            collect_data["일자"] = datetime.today().strftime("%Y%m%d")
            self.res = collect_data

        else:

            for key in self.output_param:
                data = self.dynamicCall(
                    "GetCommData(QString, QString, int, QString)",
                    trCode,
                    requestName,
                    0,
                    key,
                )
                collect_data[key].append(data.strip())

            collect_data = pd.DataFrame(collect_data)

        self.tr_request_Loop.exit()

    def getCommRealData(self, code, fid):
        """
        실시간 데이터 소싱
        이 메서드는 반드시 receiveRealData() 이벤트 메서드가 호출될 때, 그 안에서 사용해야 합니다.
        :param code: string - 종목코드
        :param fid: - 실시간 타입에 포함된 fid
        :return: string - fid에 해당하는 데이터
        """
        value = self.dynamicCall("GetCommRealData(QString, int)", code, fid)
        return value


class KiwoomAPIDelayCheck:

    """시간조회 체크"""

    def __init__(self, logger=True):
        """
        Kiwoom API 요청 제한을 피하기 위해 요청을 지연

        params
        logger: Kiwoom Class의 logger - defalut=None
        """
        # 1초에 5회, 1시간에 1,000회 제한
        self.rqHistory = deque(maxlen=999)

        if logger:
            self.logger = logger

    def checkDelay(self, signal=None):
        """
        TR 1초 5회 제한을 피하기 위해, 조회 요청을 지연합니다.
        """
        if signal == "pass":
            time.sleep(5)
        else:
            time.sleep(0.3)  # 기본적으로 요청 간에는 0.3초 delay

        if len(self.rqHistory) < 5:
            pass
        else:
            # 1초 delay (5회)
            oneSecRqTime = self.rqHistory[-5]

            # 1초 이내에 5번 요청하면 delay
            while True:
                RqInterval = time.time() - oneSecRqTime
                if RqInterval > 1.1:
                    break

        # 1hour delay (1000회 넘으면 오류)
        if len(self.rqHistory) == 999:
            oneHourRqTime = self.rqHistory[0]
            oneHourRqInterval = time.time() - oneHourRqTime

            if oneHourRqInterval < 3610:
                delay = 3610 - oneHourRqInterval

                if self.logger:
                    # self.logger.warning('{} checkRequestDelay: Request delayed by {} seconds'.format(datetime.now(), delay))
                    print(
                        "{} checkRequestDelay: Request delayed by {} seconds".format(
                            datetime.now(), delay
                        )
                    )
                    time.sleep(delay)

        # 새로운 request 시간 기록
        self.rqHistory.append(time.time())
