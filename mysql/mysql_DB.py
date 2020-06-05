import sqlalchemy
import pandas as pd


class MySql:
    def __init__(self, host, port, user, pwd, schema):
        self.con = self.connect_sqlalchemy_engine(host, port, user, pwd, schema)

    def connect_sqlalchemy_engine(self, host, port, user, pwd, schema):
        query = "mysql://{}:{}@{}:{}/{}?charset=utf8"
        query = query.format(user, pwd, host, port, schema)
        return sqlalchemy.create_engine(query, pool_recycle=1)

    def execute(self, sql, **kwargs):
        return self.con.execute(sql, **kwargs)

    def read_sql(self, sql, **kwargs):
        return pd.read_sql(sql, self.con, **kwargs)

    def write_pd(self, df, table, **kwargs):
        return df.to_sql(name=table, con=self.con, **kwargs)

    def close_con(self):
        return self.con.dispose()
