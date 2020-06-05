import pandas as pd
import numpy as np
import requests as req

from itertools import chain
from functools import reduce
from bs4 import BeautifulSoup as bs
from html_table_parser import parser_functions as parser


""" DB 데이터 적재 후 가져와서 계산 ※날짜 유의※ 
    데이터 불러와서 재무비율 직접 산출 """


class FinRatio:
    def __init__(self, stock_data, fin_data, disclosure_date):

        self.stock_data = dict(stock_data)  # > from db
        self.fin_data = dict(fin_data)
        self.disclosure_date = disclosure_date

    def annual(self, Q_data):  # 연율화
        return Q_data * 4

    def avg(self, data):  # 전기(분기)+당기(분기) 평균
        prev = data.shift(1)
        avg = (prev + data) / 2
        return avg

    def match_index(self, fin_dates, daily):

        new_daily = []
        for q in fin_dates:
            last_bq = sorted(filter(lambda x: x <= q, daily))
            new_daily.append(last_bq[-1])
        return new_daily

    def cal_ratio(self, ticker, freq="Q"):

        ## 공시 일자 기준으로 산출

        fin_data, stock_data = {}, {}
        for key, value in self.stock_data.items():
            new_index = self.match_index(self.disclosure_date[ticker], value.index)
            t_val = value.loc[new_index, ticker]
            t_val.index = new_index
            stock_data.update({key: t_val})

        for key, value in self.fin_data.items():
            t_val = value[ticker]
            t_val.index = self.disclosure_date[ticker]
            fin_data.update({key: t_val})

        # 가치 지표
        PER = self.stock_data["종가"] / self.fin_data["EPS"].map(
            lambda x: x if x != 0 else None
        )
        PBR = self.stock_data["종가"] / self.fin_data["BPS"].map(
            lambda x: x if x != 0 else None
        )
        PSR = self.stock_data["종가"] / self.fin_data["SPS"].map(
            lambda x: x if x != 0 else None
        )
        PCR = self.stock_data["종가"] / self.fin_data["CFPS"].map(
            lambda x: x if x != 0 else None
        )
        EV = (
            (self.stock_data["시가총액"] * 1000)
            + self.fin_data["비지배주주지분"]
            + (self.fin_data["순차입부채"].map(lambda x: x if x > 0 else 0))
        )
        EBITDA = self.fin_data["영업이익"] + self.fin_data["유무형자산상각비"]

        # 수익성
        매출총이익률 = self.fin_data["매출총이익"] / self.fin_data["매출액"] * 100
        영업이익률 = self.fin_data["영업이익"] / self.fin_data["매출액"] * 100
        순이익률 = self.fin_data["당기순이익"] / self.fin_data["매출액"] * 100
        EBIT마진율 = EBITDA / self.fin_data["매출액"] * 100
        if freq == "Q":
            ROE = (
                self.annual(self.fin_data["지배주주순이익"])
                / self.avg(self.fin_data["지배주주지분"])
                * 100
            )
        else:
            ROE = (
                self.fin_data["지배주주순이익"] / self.avg(self.fin_data["지배주주지분"]) * 100
            )  # 연 기준
        if freq == "Q":
            ROA = (
                self.annual(self.fin_data["당기순이익"])
                / self.avg(self.fin_data["자산총계"])
                * 100
            )
        else:
            ROA = self.fin_data["당기순이익"] / self.avg(self.fin_data["자산총계"]) * 100  # 연 기준

        Accrual = (
            self.fin_data["세전계속사업이익"] - self.fin_data["영업활동으로인한현금흐름"]
        ) / self.fin_data["자산총계"]
        GPA = self.fin_data["매출총이익"] / self.fin_data["자산총계"] * 100

        # 성장성
        매출액증가율 = (self.fin_data["매출액"] / self.fin_data["매출액"].shift(1) - 1) * 100
        영업이익증가율 = (self.fin_data["영업이익"] / self.fin_data["영업이익"].shift(1) - 1) * 100
        순이익증가율 = (self.fin_data["당기순이익"] / self.fin_data["당기순이익"].shift(1) - 1) * 100
        자산총계증가율 = (self.fin_data["자산총계"] / self.fin_data["자산총계"].shift(1) - 1) * 100
        유동자산증가율 = (self.fin_data["유동자산"] / self.fin_data["유동자산"].shift(1) - 1) * 100
        자기자본증가율 = (self.fin_data["자본총계"] / self.fin_data["자본총계"].shift(1) - 1) * 100
        EPS증가율 = (
            self.fin_data["EPS"].map(lambda x: x if x != 0 else None)
            / self.fin_data["EPS"].map(lambda x: x if x != 0 else None).shift(1)
            - 1
        ) * 100
        PEGR = PER / EPS증가율

        # 안정성
        자기자본비율 = self.fin_data["자본총계"] / self.fin_data["자산총계"] * 100
        순부채비율 = self.fin_data["순차입부채"] / self.fin_data["자본총계"] * 100
        부채비율 = self.fin_data["부채총계"] / self.fin_data["자본총계"] * 100
        유동비율 = self.fin_data["유동자산"] / self.fin_data["유동부채"] * 100
        당좌비율 = self.fin_data["당좌자산"] / self.fin_data["유동부채"] * 100
        이자보상배율 = self.fin_data["영업이익"] / self.fin_data["이자비용(비영업)"]
        유보율 = (
            (self.fin_data["지배주주순자산(자사주차감전)"] - self.fin_data["자본금"])
            / self.fin_data["자본금"]
            * 100
        )
        if freq == "Q":
            자산총계회전율 = self.annual(self.fin_data["매출액"]) / self.avg(
                self.fin_data["자산총계"]
            )
        else:
            자산총계회전율 = self.fin_data["매출액"] / self.avg(self.fin_data["자산총계"])  # 연 기준
        if freq == "Q":
            자기자본회전율 = self.annual(self.fin_data["매출액"]) / self.avg(
                self.fin_data["자본총계"]
            )
        else:
            자기자본회전율 = self.fin_data["매출액"] / self.avg(self.fin_data["자본총계"])  # 연 기준
        현금배당성향_보통 = self.fin_data["배당금(보통주)"] / self.fin_data["지배주주순이익"] * 100  ## > 배당금
        현금배당성향_우선 = self.fin_data["배당금(우선주)"] / self.fin_data["지배주주순이익"] * 100  ## > 배당금
        배당수익률_보통 = self.fin_data["DPS(보통주)"] / self.stock_data["종가"] * 100
        배당수익률_우선 = self.fin_data["DPS(우선주)"] / self.stock_data["종가"] * 100
        # ROIC = self.annual(self.fin_data["영업이익"]) / (
        #     self.fin_data["유형자산"] + self.fin_data["유동자산"] - self.fin_data["유동부채"]
        # )
        지속가능성장률 = 유보율 * (1 + 부채비율) * 순이익률 * 자산총계회전율

        newFinRatio = {}  # pd.DataFrame()
        for value, key in zip(
            [
                PER,
                PBR,
                PSR,
                PCR,
                매출총이익률,
                영업이익률,
                순이익률,
                EV,
                EBITDA,
                EBIT마진율,
                ROE,
                ROA,
                Accrual,
                GPA,
                매출액증가율,
                영업이익증가율,
                순이익증가율,
                자산총계증가율,
                유동자산증가율,
                자기자본증가율,
                PEGR,
                자기자본비율,
                순부채비율,
                부채비율,
                유동비율,
                당좌비율,
                이자보상배율,
                유보율,
                자산총계회전율,
                자기자본회전율,
                현금배당성향_보통,
                현금배당성향_우선,
                배당수익률_보통,
                배당수익률_우선,
                # ROIC,
                지속가능성장률,
            ],
            [
                "PER",
                "PBR",
                "PSR",
                "PCR",
                "매출총이익률",
                "영업이익률",
                "순이익률",
                "EV",
                "EBITDA",
                "EBIT마진율",
                "ROE",
                "ROA",
                "Accrual",
                "GPA",
                "매출액증가율",
                "영업이익증가율",
                "순이익증가율",
                "자산총계증가율",
                "유동자산증가율",
                "자기자본증가율",
                "PEGR",
                "자기자본비율",
                "순부채비율",
                "부채비율",
                "유동비율",
                "당좌비율",
                "이자보상배율",
                "유보율",
                "자산총계회전율",
                "자기자본회전율",
                "현금배당성향_보통",
                "현금배당성향_우선",
                "배당수익률_보통",
                "배당수익률_우선",
                # "ROIC",
                "지속가능성장률",
            ],
        ):
            newFinRatio.update({key: value})

        return pd.DataFrame(newFinRatio)

    def search_NICE(self, ticker):

        attribute_ls = ["i1301", "i1401"]  # , 'i1501', 'i1701', 'i1801']
        param_ls = [
            (7, 12, "배당내역"),
            (6, 11, "주당가치지표"),
        ]  # , (5, 6, '내재가치지표'), (7, 11, 'EVA지표'), (7, 6, 'EBITDA지표')]
        param_dict = dict(zip(attribute_ls, param_ls))

        def get_table_gen_from_nice(ticker, attr):

            # for tick in tick_df:
            try:
                url = "http://media.kisline.com/investinfo/mainInvestinfo.nice?paper_stock={}&nav=3".format(
                    ticker
                )
                res = req.get(url)

                bs_data = bs(res.content, "html.parser")
                table_data = bs_data.find("div", {"id": attr})

                yield pd.DataFrame(parser.make2d(table_data))  # , name

            except:
                pass

        for attr in list(param_dict.keys()):

            gen = get_table_gen_from_nice(ticker, attr)

            target_ls = []
            non_target_ls = []

            for idx, data in enumerate(gen):
                print(data)
                if data[0].shape[0] == param_dict[attr][0]:  #
                    print(data)
                    target_ls.append(data)
                else:
                    non_target_ls.append(data)

                if idx % 500 == 3:
                    print(len(target_ls), len(non_target_ls))
                    print(np.array(target_ls).shape)

            col_ls = []

            for name in target_ls:
                col_ls.append([name[1] for i in range(param_dict[attr][1])])  #

            df = pd.DataFrame(
                reduce(lambda x, y: np.hstack([x, y]), np.array(target_ls)[:, 0])
            )
            col = list(chain(*col_ls))

            df.columns = col
            df.to_csv("./{}.csv".format(param_dict[attr][2]))
