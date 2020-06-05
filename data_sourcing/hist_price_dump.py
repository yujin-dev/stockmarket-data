import pandas as pd
import numpy as np
import os
from mysql.mysql_DB import MySql
from mysql.config import DBConfig
import pickle
from datetime import datetime, timedelta


class DumpData:


    def __init__(self, data_set, k_dates):

        self.data_set = data_set
        self.k_dates = k_dates
        print(data_set, k_dates)
    
    def trading_date(self, vol, market_info):

        """
        시세 관련 데이터 정리
        # 거래량 = 0(정리매매 구간 포함)
        # 각 거래소( 코스피 , 코스닥 ) 상장된 구간에만 데이터 유지

        데이터 존재하는 구간만 정리해서 종목별 date를 추출함
        :return: dictionary( 종목별 데이터 유효한 기간만 정리 )
        """
        # vol = trade_data['거래량']
        vol_0 = vol.applymap(lambda x: np.nan if x == 0 or np.isnan(x) else x)

        data_exist = {}
        for stock in vol_0.columns:
            normal_vol = vol_0[stock].dropna().index
            issue_date = market_info[stock].dropna().index
            add_index = sorted(set(normal_vol).intersection(set(issue_date)))
            data_exist[stock] = add_index

        return data_exist


    def dump_dataset(self):

        print(self.data_set["시장구분"].drop_duplicates())
        [print(f"{k} shape : {v.shape}") for k, v in self.data_set.items()]

        sql_config = getattr(DBConfig, "financeteam_stockmarket")

        connect_sql = MySql(
            host=sql_config["host"],
            port=sql_config["port"],
            user=sql_config["user"],
            pwd=sql_config["password"],
            schema=sql_config["schema"],
        )

        """ 거래량이나 종가 합이 0이 아닌 종목들( 정리매매 포함 ) """
        adj_close = self.data_set["수정주가"]
        valid_stocks = np.nansum(adj_close, axis=0)
        possible_stocks = np.array(adj_close.columns)[valid_stocks != 0]

        """ 시장구분 nan이 아닌 & 거래량 0 이 아닌 구간만 포함 """
        trading_dates = self.trading_date(self.data_set["거래량"], self.data_set["시장구분"])

        def time_series(stock_data, nan_date):
            stock_data.loc[nan_date] = np.nan
            return stock_data

        is_common = lambda x: 1 if x[-1] == "0" else 0

        for stock in possible_stocks:

            nan_date = sorted(set(self.k_dates) - set(trading_dates[stock]))
            print(stock, nan_date)
            obj_ao = np.array(
                time_series(self.data_set["수정시가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            adj_lo = np.array(
                time_series(self.data_set["수정저가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            adj_hg = np.array(
                time_series(self.data_set["수정고가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            adj_cl = np.array(
                time_series(self.data_set["수정주가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            cl = np.array(
                time_series(self.data_set["종가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            v = np.array(
                time_series(self.data_set["거래량"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            mkt_cap = np.array(
                time_series(self.data_set["시가총액"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            f_net = np.array(
                time_series(self.data_set["외인순매수"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            id_net = np.array(
                time_series(self.data_set["개인순매수"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            ins_net = np.array(
                time_series(self.data_set["기관순매수"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            mar = np.array(
                self.data_set["시장구분"][stock].apply(lambda x: 1 if x == "KSE" else 0)
            )

            tr_date = list(map(lambda x: x.replace("-", ""), self.k_dates))
            dump_data = pd.DataFrame(
                {
                    "STOCK_CODE": [stock for _ in range(len(tr_date))],
                    "ADJ_OPEN": obj_ao,
                    "ADJ_LOW": adj_lo,
                    "ADJ_HIGH": adj_hg,
                    "ADJ_CLOSE": adj_cl,
                    "CLOSE": cl,
                    "MARKET_CAP": mkt_cap,
                    "TVOL": v,
                    "FOREIGN_NET": f_net,
                    "INDIVIDUAL_NET": id_net,
                    "INSTITUTION_NET": ins_net,
                    "MARKET": mar,
                    "COMMON": [is_common(stock) for _ in range(len(tr_date))],
                },
                index=tr_date,
            )

            try:
                connect_sql.write_pd(
                    df=dump_data,
                    table="hist_price",
                    if_exists="append",
                    index=True,
                    index_label="TRD_DD",
                )

            except:
                print(stock)

        connect_sql.close_con()