import pandas as pd
import numpy as np
import os
import pickle
from dataguide_refresh import update_dataguide
from datetime import datetime, timedelta
from data_sourcing.hist_price_dump import DumpData
from data_setting import SortStockData


class SeriesData:

    def __init__(self):

        self.check_holiday = list(pd.read_csv('../2020_holiday.csv', index_col=0, engine='python').index)

    def organize_series(self, dir):

        dataset = {}

        for item_name in os.listdir(dir):

            item_data = pd.read_pickle(os.path.join(dir, item_name))
            item_name = item_name.split(".")[0].split("(")[0].strip()

            if len(k_dates) > 1:
                dataset[item_name] = item_data.loc[k_dates[0] : k_dates[-1]]

            elif len(k_dates) == 1:
                dataset[item_name] = pd.DataFrame(item_data.loc[k_dates[0]]).T

        return dataset


    def refresh_series(self, , k_dates):

        ''' DataGuide - Time Series '''

        if refresh:
            ### DataGuide 로그인 되어있어야 함
            update_dataguide("series_data.xlsm", "series_data.xlsm!DoAllSheetRefresh")
        organized_data = SortStockData("series_data.xlsm").sort_stock_data(sort_type='item', save_type='pickle')
        
        dir = organized_data.save_dir

        data_set = self.organize_series(dir)

        return DumpData(data_set, k_dates)#.dump_dataset()



if __name__ == "__main__":

    SeriesData().refresh_series(refresh=False)

