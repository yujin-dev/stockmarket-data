import pandas as pd
import numpy as np
import os
import pickle
from datetime import datetime, timedelta
from dataguide_refresh import update_dataguide
from data_sourcing.hist_price_dump import DumpData
from data_sourcing.data_check.daum_price import Daum_AdjPrice

class NewestData:

    def __init__(self):

        self.check_holiday = list(pd.read_csv('../2020_holiday.csv', index_col=0, engine='python').index)


    def organize_current(self, data, today):

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


    def refresh_data(self, refresh=True):

        ''' DataGuide Cross Sectional '''

        if refresh:
            ### DataGuide 로그인 되어있어야 함
            update_dataguide("compare_data.xlsm", "compare_data.xlsm!DoAllSheetRefresh")

        while True:

            data = pd.read_excel("compare_data.xlsm", thousands=",", skiprows=[1,2,3,4])

            data.columns = data.iloc[0]
            data.drop(0, inplace=True)
            cur_date, cur_time = data.columns[1].split(' ')[2:]
            Y, M, D = cur_date.split('-')
            h, m, s = cur_time.split(':')
            date = datetime(int(Y), int(M), int(D), int(h), int(m), int(s), 00000)  

            if '0000' < data.columns[1].split(' ')[3].replace(':', '')[:4] < '0730':
                ## 자정 이후 단일가 매매 시작( 07:30 AM ) 이전에는 이전 날짜
                date = date - timedelta(1)

            date = date.strftime("%Y-%m-%d")
            data_set = self.organize_current(data, date)
            k_dates = [date]
            
            price_crawler = Daum_AdjPrice(date.replace('-', ''), list(data.index))
            price_crawler.run()
            check_adj = pd.DataFrame(price_crawler.total_adjusted, index=[date])
            check_update = data_set['수정주가'] - check_adj

            if check_update[['A005930', 'A000660', 'A000020']].sum().sum()  != 0:
                ####### telegram messeage #######
                print('삼성전자 - SK하이닉스 - 동화약품 수정주가 비교 >> 갱신되지 않은 것으로 보입니다.\n DataGuide Refresh 필요합니다. ')
                update_dataguide("compare_data.xlsm", "compare_data.xlsm!DoAllSheetRefresh")
                continue

            else: 
                break

        check_weekend = pd.date_range(end=date, freq="B", periods=1)[0].strftime("%Y-%m-%d")

        if date not in self.check_holiday and date == check_weekend:
            print(date, 'will be updated')
            return DumpData(data_set, k_dates).dump_dataset()
        
        else: 
            print(date, ' is holiday')
            return


if __name__ == "__main__":


    """ 가장 최근 데이터 """
    NewestData().refresh_data(refresh=False)
