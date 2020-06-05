import pandas as pd
import numpy as np
import os
import pickle
from datetime import datetime, timedelta
from dataguide_refresh import update_dataguide
from data_sourcing.naver_current_vol import GetVol
from data_sourcing.hist_price_dump import DumpData
from data_sourcing.data_check.daum_price import Daum_AdjPrice

class CurrentData:

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


    def refresh_current(self, refresh=True):

        ''' DataGuide Cross Sectional '''

        if refresh:
            ### DataGuide 로그인 되어있어야 함
            update_dataguide("compare_data.xlsm", "compare_data.xlsm!DoAllSheetRefresh")

        while True:
            data = pd.read_excel("compare_data.xlsm", index_col=0, header=5, thousands=",")
            date = datetime.today()

            ### 3시 이전으로 전날 데이터 덤프
            if datetime.now().strftime("%H%M") < "1500":
                date = date - timedelta(1)

            date = '2020-06-04'#date.strftime("%Y-%m-%d")
            data_set = self.organize_current(data, date)
            k_dates = [date]
            
            price_crawler = Daum_AdjPrice(date.replace('-', ''), list(data.index))
            price_crawler.run()
            check_adj = pd.DataFrame(price_crawler.total_adjusted, index=[date])
            check_update = data_set['수정주가'] - check_adj

            if check_update[['A005930', 'A000660', 'A000020']].sum().sum()  != 0:
                print('삼성전자 - SK하이닉스 - 동화약품 수정주가 비교 >> 갱신되지 않은 것으로 보입니다.\n DataGuide Refresh 필요합니다. ')
                update_dataguide("compare_data.xlsm", "compare_data.xlsm!DoAllSheetRefresh")
            else: 
                break
       
        if (check_update[check_update==0].sum().sum()) > 0: 
            print(check_update[check_update==0].sum())
            print('DataGuide 갱신 여부를 확인할 필요가 있습니다. ')

        
        if date not in self.check_holiday:
            print(date, 'will be updated')

            ### 거래량 크롤링 >> 갱신
            stock_list = list(map(lambda  x: x.split('A')[-1], data.index))
            get_volume = GetVol(stock_list)
            get_volume.run()
            data_set.update({'거래량':pd.DataFrame(get_volume.total_vol, index=[date])})
            print('업데이트 완료')

            return DumpData(data_set, k_dates)#.dump_dataset()
        
        else: 
            print(date, ' is holiday')
            return


if __name__ == "__main__":


    """ 가장 최근 데이터 """
    CurrentData().refresh_current(refresh=False)
