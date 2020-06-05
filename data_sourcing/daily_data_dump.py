import pandas as pd
import numpy as np
import os
from mysql.mysql_DB import MySql
from mysql.config import DBConfig
import pickle
from datetime import datetime, timedelta
from dataguide_refresh import update_dataguide


class DumpData:
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

    def dump_dataset(self, data_set, k_dates):

        print(data_set["시장구분"].drop_duplicates())
        [print(k, v.shape) for k, v in data_set.items()]

        sql_config = getattr(DBConfig, "financeteam_stockmarket")

        connect_sql = MySql(
            host=sql_config["host"],
            port=sql_config["port"],
            user=sql_config["user"],
            pwd=sql_config["pwd"],
            schema=sql_config["schema"],
        )

        """ 거래량이나 종가 합이 0이 아닌 종목들( 정리매매 포함 ) """
        adj_close = data_set["수정주가"]
        valid_stocks = np.nansum(adj_close, axis=0)
        possible_stocks = np.array(adj_close.columns)[valid_stocks != 0]

        """ 시장구분 nan이 아닌 & 거래량 0 이 아닌 구간만 포함 """
        trading_dates = self.trading_date(data_set["거래량"], data_set["시장구분"])

        def time_series(stock_data, nan_date):
            stock_data.loc[nan_date] = np.nan
            return stock_data

        is_common = lambda x: 1 if x[-1] == "0" else 0

        for stock in possible_stocks:

            nan_date = sorted(set(k_dates) - set(trading_dates[stock]))
            print(stock, nan_date)
            obj_ao = np.array(
                time_series(data_set["수정시가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            adj_lo = np.array(
                time_series(data_set["수정저가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            adj_hg = np.array(
                time_series(data_set["수정고가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            adj_cl = np.array(
                time_series(data_set["수정주가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            cl = np.array(
                time_series(data_set["종가"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            v = np.array(
                time_series(data_set["거래량"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            mkt_cap = np.array(
                time_series(data_set["시가총액"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            f_net = np.array(
                time_series(data_set["외인순매수"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            id_net = np.array(
                time_series(data_set["개인순매수"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            ins_net = np.array(
                time_series(data_set["기관순매수"][stock], nan_date).apply(
                    lambda x: np.int64(x) if np.isnan(x) == False else np.nan
                )
            )
            mar = np.array(
                data_set["시장구분"][stock].apply(lambda x: 1 if x == "KSE" else 0)
            )

            tr_date = list(map(lambda x: x.replace("-", ""), k_dates))
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

    def series_data(self, dir, k_dates):
        def organize_series(dir):

            dataset = {}

            for item_name in os.listdir(dir):

                item_data = pd.read_pickle(os.path.join(dir, item_name))
                item_name = item_name.split(".")[0].split("(")[0].strip()

                if len(k_dates) > 1:
                    dataset[item_name] = item_data.loc[k_dates[0] : k_dates[-1]]

                elif len(k_dates) == 1:
                    dataset[item_name] = pd.DataFrame(item_data.loc[k_dates[0]]).T

            return dataset

        data_set = organize_series(dir)
        print(data_set)

        self.dump_dataset(data_set, k_dates)

    def current_data(self, refresh=True):
        def organize_current(data):

            dataset = {}
            data_cols = set(data.columns) - set(["Name"])
            for item_name in data_cols:

                item_data = data[item_name]
                if "순매수대금(개인)" in item_name:
                    item_name = "개인순매수"
                elif "순매수대금(기관계)" in item_name:
                    item_name = "기관순매수"
                elif "순매수대금(등록외국인)" in item_name:
                    item_name = "등록외국인순매수"
                elif "순매수대금(외국인계)" in item_name:
                    item_name = "외인순매수"

                else:
                    item_name = item_name.split(".")[0].split("(")[0].strip()

                item_data = pd.DataFrame(item_data).T
                item_data.index = [today]
                dataset[item_name] = item_data

            return dataset

        if refresh:
            update_dataguide("compare_data.xlsm", "compare_data.xlsm!DoAllSheetRefresh")

        data = pd.read_excel("compare_data.xlsm", index_col=0, header=5, thousands=",",)

        today = datetime.today()

        ### 3시 이전으로 전날 데이터 덤프
        if datetime.now().strftime("%H%M") < "1500":
            today = today - timedelta(1)

        today = today.strftime("%Y-%m-%d")

        data_set = organize_current(data)
        k_dates = [today]
        print(today)
        print(data_set)

        self.dump_dataset(data_set, k_dates)


if __name__ == "__main__":

    def read_pickle(data_name):
        with open(data_name, "rb") as fr:
            data = pickle.load(fr)
        return data

    """ 가장 최근 데이터 """
    DumpData().current_data(refresh=False)
