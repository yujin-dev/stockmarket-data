import h5py
import pandas as pd


class DailyHDF5Reader:



    def __init__(self):
        pass


    def validate_attr(self, name, attrs, input:list):

        check = set(input).difference(attrs)
        if len(check):
            raise KeyError(f"invalid {name} {sorted(check)}")


    def get_daily_data(self, date_from=None, date_to=None, items:list=[], code: list=[], asset_type="stock"):
        """
        :param date_from: start date
        :param date_to: end date
        :param items: item list
        :param code: stock code list
        :param asset_type: [ stock, index, futures ]
        :return:
        """

        years = [i for i in range(int(date_from[:4]), int(date_to[:4])+1)]

        for year in years:

            with h5py.File("data/daily_{}.h5".format(year), "r") as f:

                arr = f["/{}/data".format(asset_type)]
                att_index = list(f["/{}/index".format(asset_type)][:])
                att_columns = list(map(lambda x: x.decode(), f["/{}/columns".format(asset_type)]))
                att_items = list(f["/{}".format(asset_type)].attrs["items"])

                date_range = [int(i) for i in pd.date_range(start=date_from, end=date_to).strftime("%Y%m%d")]
                dates = [int(i) for i in sorted(set(att_index).intersection(date_range))]

                self.validate_attr("items", att_items, items)
                self.validate_attr("dates", att_index, dates)
                self.validate_attr("code", att_columns, code)

                if len(items)> 0:
                    items_num = [att_items.index(i) for i in items]
                    arr = arr[items_num, :, :]
                else:
                    items = att_items

                dates_num = [att_index.index(i) for i in dates]
                if len(dates_num) > 0:
                    arr = arr[:, dates_num]
                else:
                    dates = att_index

                if len(code) > 0:
                    stocks_num = [att_columns.index(i) for i in code]
                    arr = arr[:, :, stocks_num]
                else:
                    code = att_columns

                df = {}
                for key in items:
                    n = list(f["/{}".format(asset_type)].attrs["items"]).index(key)
                    #df.append(pd.DataFrame(arr[n], index=dates, columns=code))
                    df[key] = pd.DataFrame(arr[n], index=dates, columns=code)

                yield df # -> dict

        yield None


if __name__ == "__main__":


    getter = DailyHDF5Reader()
    # df = getter.get_daily_data(date_from="19900101", date_to="19901010", items=['comm_stk_qty', 'pref_stk_qty', 'per', 'pbr', 'pcr', 'psr'],
    #                            code=['000010', '000020', '000030', '000040', '000050', '000060',
    #                                        '000070', '000080', '000090', '000100', '000110', '000120',
    #                                        '000130', '000140', '000150', '000160', '000170', '000180',
    #                                        '000200', '000210', '000220', '000230', '000240', '000270'])
    df = getter.get_daily_data(date_from="20201214", date_to="20201231", asset_type="futures") # futures , index

    while True:
        nxt = next(df)
        if nxt is None:
            break
        for key, value in nxt.items():
            print(key)
            print(value.head())
