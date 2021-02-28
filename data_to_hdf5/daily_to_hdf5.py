import h5py
import numpy as np
import pandas as pd
import os
import pickle


def read_chunk(file_name, chunksize):
    return pd.read_csv(file_name, index_col=0, chunksize=chunksize)


def save_pkl(file_name, data_set):
    with open(file_name, 'wb') as f:
        pickle.dump(data_set, f)


class TotalDailySaver:

    futures = ["futures"]
    index = ["index_data"]
    stock = [ "price", "etf", "market_capital", "factor","buy_sell"]
    category = {"stock": stock, "futures": futures, "index": index}

    yearly_asset_type_dir = "spilt_yearly"
    hdf5_dir = "data"
    os.makedirs(yearly_asset_type_dir, exist_ok=True)
    os.makedirs(hdf5_dir, exist_ok=True)


    def __init__(self, start=1990, end= 2022, data_dir=r'D:\fnguide'):
        self.date_range = [str(year) for year in range(int(start), int(end))]  # 2022
        self.dir = data_dir


    @staticmethod
    def check_mem():
        total_data = os.listdir(TotalDailySaver.hdf5_dir)
        total_mem = [os.path.getsize(os.path.join(TotalDailySaver.hdf5_dir, i))/(1024**2) for i in total_data]
        print(f"TOTAL MEM : {sum(total_mem)} MB")



    def build_stock_total_data(self, file_name="stock_total"):

        total = {}
        for file in self.stock:
            total[file] = pd.read_csv(os.path.join(self.dir, file) + '.csv', index_col=0)
        save_pkl(f"{file_name}.pkl", total)


    def build_yearly_pkl_all(self, asset_type = ['stock', 'futures', 'index']):

        data_dict = {}
        if "stock" in asset_type:
            stock = pd.read_pickle("stock_total.pkl")
            data_dict["stock"] = {key: stock[key] for key in ['price', 'buy_sell', 'market_capital', 'factor', 'etf']}
        if "futures" in asset_type:
            data_dict["futures"] = pd.read_csv(os.path.join(self.dir, "futures.csv"), index_col=0)
        if "index" in asset_type:
            data_dict["index"] = pd.read_csv(os.path.join(self.dir, "index_data.csv"), index_col=0)

        for name, data_set in data_dict.items():
            [self.build_yearly_pkl(year, name, data_set) for year in self.date_range ]


    @classmethod
    def build_yearly_pkl(cls, year:str, name:str, data_set):
        print(name)
        def rebuild(data, year):

            tg = data[data["date"].map(lambda x: str(x)[:4] == year)]
            if not tg.empty:
                if name == 'stock':
                    tg['code'] = tg['code'].astype('string').map(lambda x: x.zfill(6))
                elif name == 'futures': # futures인 경우 code를 name으로 대체함
                    tg['code'] = tg['name'].astype('string')
                elif name == 'index':
                    tg['code'] = tg['code'].astype('string')

                del tg["name"]
                return tg

        print(year, "...")

        if isinstance(data_set, dict):

            yearly_df = pd.DataFrame()
            for item, df in data_set.items():
                tg = rebuild(df, year)
                if tg is not None:
                    if yearly_df.empty:
                        yearly_df = tg
                    else:
                        cols_to_use = tg.columns.difference(yearly_df.columns.difference(["date", "code"]))
                        if len(cols_to_use) != len(tg.columns):
                            print(tg.columns, '->', cols_to_use)
                        yearly_df = pd.merge(yearly_df, tg[cols_to_use], how = "outer", left_on=["date", "code"], right_on=["date", "code"])
                print("✓", item, " done")
            if not yearly_df.empty:
                yearly_df.to_pickle(os.path.join(cls.yearly_asset_type_dir, "{}_{}.pkl".format(year, name)))

        elif isinstance(data_set, pd.DataFrame):
            yearly_df = rebuild(data_set, year)
            if yearly_df is not None:
                yearly_df.to_pickle(os.path.join(cls.yearly_asset_type_dir, "{}_{}.pkl".format(year, name)))


    def build_data_all(self):
        [self.build_data(year) for year in self.date_range]


    @classmethod
    def build_data(cls, year):

        print(year, "...")

        with  h5py.File("{}/daily_{}.h5".format(cls.hdf5_dir, year), "w") as f:

            for name in cls.category.keys():
                print(name)
                yearly_data = []
                try:
                    df = pd.read_pickle(os.path.join(cls.yearly_asset_type_dir, "{}_{}.pkl".format(year, name)))

                except FileNotFoundError:
                    print(f"{year}_{name} doesn't exist")
                    continue

                index, columns = set({}), set({})
                items = []
                for col in df.columns:
                    if col not in ["date", "code", "name"]:
                        data = pd.pivot_table(df, index='date', columns="code", values=col)
                        data.sort_index(axis=0, inplace=True)
                        data.sort_index(axis=1, inplace=True)
                        yearly_data.append(data) # , dtype=np.float64)
                        index.update(data.index)
                        columns.update(data.columns)
                        items.append(col)

                def rebuild(frame, base_col, base_index):
                    diff_col = base_col.difference(frame.columns)
                    frame = pd.concat([frame, pd.DataFrame(columns=diff_col)], axis=1)
                    diff_in = base_index.difference(frame.index)
                    frame = pd.concat([frame, pd.DataFrame(index=diff_in)], axis=0)
                    frame.sort_index(axis=0, inplace=True)
                    frame.sort_index(axis=1, inplace=True)
                    frame.fillna(np.nan, inplace=True)
                    return np.array(frame,  dtype=np.float64)

                for n, frame in enumerate(yearly_data):
                    yearly_data[n] = rebuild(frame, columns, index)

                arr = np.array(yearly_data, dtype=np.float64)
                frame_info = sorted(index), sorted(columns)

                f.create_group(name)
                try:
                    f["/{}/data".format(name)] = arr
                except OSError:
                    f["/{}/data".format(name)][:] = arr
                except Exception as e:
                    print(e)

                f["/{}/index".format(name)] = frame_info[0]
                f["/{}/columns".format(name)] = np.array(frame_info[1], dtype=h5py.string_dtype(encoding='utf-8'))
                f["/{}".format(name)].attrs["shape"] = arr.shape
                f["/{}".format(name)].attrs["items"] = items

                print("✓", name, " done")

            return

if __name__ == "__main__":

    tds = TotalDailySaver()
    #tds.build_stock_total_data()

    tds.build_yearly_pkl_all(asset_type=["index"])
    tds.build_data_all()

    tds.check_mem()