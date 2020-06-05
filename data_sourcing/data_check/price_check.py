import os
import pandas as pd
import numpy as np
import sys


class CheckData:

    """ 가격 기본 조건( 상한 - 하한 ) 확인 """

    def check_limit(self, data, cutoff, start_date=None, end_date=None):
        """ 가격 제한 폭 확인"""

        if start_date == None and end_date != None:
            종가 = data["수정주가"].loc[:end_date].iloc[:-1].shift(1).iloc[1:].fillna(1)
            저가 = data["수정저가"].loc[:end_date].iloc[:-1].iloc[1:].fillna(1)
            고가 = data["수정고가"].loc[:end_date].iloc[:-1].iloc[1:].fillna(1)

        elif start_date != None and end_date != None:
            종가 = (
                data["수정주가"]
                .loc[start_date:end_date]
                .iloc[:-1]
                .shift(1)
                .iloc[1:]
                .fillna(1)
            )
            저가 = data["수정저가"].loc[start_date:end_date].iloc[:-1].iloc[1:].fillna(1)
            고가 = data["수정고가"].loc[start_date:end_date].iloc[:-1].iloc[1:].fillna(1)

        elif start_date != None and end_date == None:
            종가 = data["수정주가"].loc[start_date:].shift(1).iloc[1:].fillna(1)
            저가 = data["수정저가"].loc[start_date:].iloc[1:].fillna(1)
            고가 = data["수정고가"].loc[start_date:].iloc[1:].fillna(1)

        check_low = 저가 > 종가 * (1 - cutoff)
        check_high = 고가 < 종가 * (1 + cutoff)

        check_low = check_low.mean().mean()
        check_high = check_high.mean().mean()
        print("cutoff = ", cutoff, check_low, check_high)

        if round(check_low, 41) != 1 or round(check_high, 1) != 1:
            raise ValueError("가격 제한폭에 맞지 않는 종목이 있습니다")

    def check_allPeriods(self, data):

        """ # 가격 제한 폭 확인
        KOSPI: 15%( ~2015/06/12 ) -> 30%( 2015/06/15~ )
        KOSDAQ: 12%( ~2005/03/25 ) -> 15%( 2005/03/28 ~ 2015/06/12) -> 30%( 2015/06/15~ )
        """

        ksp_1 = self.check_limit(data, 0.15, end_date="2015-06-12")
        ksp_2 = self.check_limit(data, 0.3, start_date="2015-06-15")

        ksq_3 = self.check_limit(data, 0.12, end_date="2005-03-25")
        ksq_4 = self.check_limit(
            data, 0.15, start_date="2005-03-28", end_date="2015-06-12"
        )
        ksq_5 = self.check_limit(data, 0.3, end_date="2015-06-15")

        return ksp_1, ksp_2, ksq_3, ksq_4, ksq_5

    def check_annual(self, data):

        """
        주기( 1Q, 2Q, 3Q, 4Q, Annual ) 포함됨
        1Q+2Q+3Q+4Q = Annual 확인( 손익계산서 , 현금흐름표 - 누적 기준으로 산출됨  )
        """
        year_sample = data.index.unique()

        for year in year_sample:
            t_val = data.loc[year]
            annuals = t_val[t_val["주기"] == "Annual"]
            quarters = t_val[t_val["주기"] != "Annual"]
            cols = [
                i
                for i in quarters.iloc[:3].dropna(how="all", axis=1).columns
                if i != "주기"
            ]
            is_equal = quarters[cols].sum() - annuals[cols]
            if (is_equal[is_equal > 1].sum(axis=1)) != 0:
                print(year, "Missing Data Exists..")


class CrossCheck:

    """ 데이터 교차 검증 """

    def __init__(self, key_list, raw_dict, update_dict):

        self.raw_set = raw_dict
        self.update_set = update_dict
        self.key_list = key_list


    def data_compare(self, key_name):

        def set_date(dates):
            try:
                return list(map(lambda x: x.strftime("%Y-%m-%d"), dates))
            except:
                return dates

        raw_d = self.raw_set[key_name]
        new_d = self.update_set[key_name]
        raw_d.index = set_date(raw_d.index)
        new_d.index = set_date(new_d.index)

        columns = set(raw_d.columns) & set(new_d.columns)
        index = set(raw_d.index) & set(new_d.index)

        raw_c = raw_d.loc[index, columns]
        new_c = new_d.loc[index, columns]
        check_err = raw_c - new_c

        check_err = check_err[check_err > 1]
        print(f'{key_name} 데이터 교차 검증 : ', check_err.dropna(how="all", axis=1).shape)



    def check(self):

        return map(self.data_compare, self.key_list)