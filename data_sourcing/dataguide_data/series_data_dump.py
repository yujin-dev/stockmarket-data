import pandas as pd
import numpy as np
import os
import pickle
from dataguide_refresh import update_dataguide
from datetime import datetime, timedelta
from data_sourcing.hist_price_dump import DumpData
from data_setting import SortStockData
from data_sourcing.data_check.daum_price import Daum_AdjPrice
from data_sourcing.data_check.compare_adj import ComparePrice
from data_sourcing.data_check.compare_adj import ComparePrice

class SeriesData:

    def __init__(self):
        
        self.check_holiday = list(pd.read_csv('../2020_holiday.csv', index_col=0, engine='python').index)


    def refresh_data(self, refresh = True):

        ''' DataGuide - Time Series '''

        date = datetime.today() - timedelta(1) # 전날
        
        if refresh:
            ### DataGuide 로그인 되어있어야 함
            update_dataguide("series_data.xlsm", "series_data.xlsm!AllSheetRefresh", date.strftime("%Y%m%d"))

        date = date.strftime("%Y-%m-%d")
        
        while True:

            sort_data = SortStockData("series_data.xlsm")
            sort_data.sort_stock_data(sort_type='item', save_type=None)
            new_data = sort_data.new_data
            
            data_set = {}
            def organize_data(key):
                value = new_data[key]
                try:
                    value.index = list(map(lambda x: x.strftime("%Y-%m-%d"), value.index))
                except:
                    pass
                value = pd.DataFrame(value.loc[date]).T
                value.index = [date]
                data_set[key.split('(')[0].strip()]=value

            list(map(organize_data, new_data.keys()))
            print(data_set)

            price_crawler = Daum_AdjPrice(date.replace('-', ''), list(data_set['수정주가'].index))
            price_crawler.run()
            check_adj = pd.DataFrame(price_crawler.total_adjusted, index=[date])
            check_update = data_set['수정주가'] - check_adj
            print(check_update)

            if check_update[['A005930', 'A000660', 'A000020']].sum().sum()  != 0:

                ####### telegram messeage #######
                print('삼성전자 - SK하이닉스 - 동화약품 수정주가 비교 >> 갱신되지 않은 것으로 보입니다.\n DataGuide Refresh 필요합니다. ')
                update_dataguide("series_data.xlsm", "series_data.xlsm!AllSheetRefresh", date.strftime("%Y%m%d"))
                continue

            else: 
                break
 
            check_weekend = pd.date_range(end=date, freq="B", periods=1)[0].strftime("%Y-%m-%d")

            if date not in self.check_holiday and date == check_weekend:
                print(date, 'will be updated')
                DumpData(data_set, [date]).dump_dataset()

                ComparePrice(date, check_adj).compare_adj_price
                ####### telegram messeage #######

            
            else: 
                print(date, ' is holiday')
                return



if __name__ == "__main__":

    SeriesData().refresh_data(refresh=False)

