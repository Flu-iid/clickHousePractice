import pandas as pd
import numpy as np
import clickhouse_connect as cc
from beep import beep
import os
from sqlalchemy import create_engine
from module.slp import Slp


class cc_dump:
    def __init__(self, name, path, chunksize=10000, engine="MergeTree", orderby="received_at"):
        self.name = name
        self.path = path
        self.chunksize = chunksize  # for dfo_csv
        self.engine = engine
        self.orderby = orderby
        self.cc_translate = {
            "object": "String",
            np.dtype('O'): "String",
            "int64": "Int",
            np.dtype("int64"): "Int",
            "float64": "Float64",
            np.dtype("float64"): "Nullable(Float64)",
            np.dtype("bool"): "Nullable(Int)",
            "datetime64": "DateTime64",
        }
        pass

    def __str__(self):
        print("cc_dump obj")

    def beep(self):
        print(os.get_terminal_size()*"-")
        beep(self.beep_time)

    def init_client(self, host="localhost",
                    port=8123,
                    username="default",
                    password="Arbit13243546576879",
                    database="default"):
        """
        if left blank it will fill with these information
        host="localhost",
        port=8123,
        username="default",
        password="Arbit13243546576879",
        database="default"
        """
        self.host = host
        self.port = port
        self.user = username
        self.password = password
        self.database = database
        self.connection = cc.get_client(
            host=self.host, port=self.port, username=self.user, password=self.password)
        tmp_flag = False
        for _ in self.connection.command("SELECT * FROM system.databases"):
            if _.strip() == self.database:
                self.connection = cc.get_client(
                    host=self.host, port=self.port, username=self.user, password=self.password, database=self.database)
                tmp_flag = True
        if not tmp_flag:
            print(database, "not existed")
            print("CREATED ", database, "DATABASE")
            self.connection.command(f"CREATE DATABASE {database}")
            self.connection = cc.get_client(
                host=self.host, port=self.port, username=self.user, password=self.password, database=self.database)
        print("connected to > ", self.connection.database)

    def has_duplicated(l: list):
        """
        to check duplicated data
        """
        s = set(l)
        if len(s) == len(l):
            return False
        return True

    def df_object(self, foo_type="csv"):
        match foo_type:
            case "csv":
                self.dfo = iter(pd.read_csv(
                    self.path, chunksize=self.chunksize))
            case "json":
                pass

    def df_modify(self, df: pd.DataFrame, option="default"):
        """
        options:
            drop_all_na: drop col if all values are NaN
            pivot: pivot dataframe without NaN
            default : drop nothing
        """
        match option:
            case "drop_all_na":
                data_series = df.isna().all()
                data_series = data_series[data_series == False]
                return df[data_series[data_series == False].index]
            case "pivot":
                df = self.df_modify(df, option="drop_all_na")
                return df.pivot(columns=list(df.columns.values)[:-1])
            case _:
                return df

    def insert_data(self, option="default", flag="v"):
        """
        options:
            drop_all_na: drop col if all values are NaN
            pivot: pivot dataframe without NaN
            default : drop nothing
        flag:
            v: verbous
        """
        dfo = self.dfo
        while True:
            try:
                df: pd.DataFrame = next(dfo)
                if option != "default":
                    df = self.df_modify(df, option=option)
                # clean commands here
                # df.dropna(inplace=True)
                # df.fillna(0, inplace=True)
                df.filter()
                df = df.fillna(np.nan).replace([np.nan], [None])
                ##
                if flag == "v":
                    print(df)
                if not self.connection.command(f"EXISTS {self.name}"):
                    keys = [f"{i} {self.cc_translate[j]}" for i,
                            j in zip(df.columns.values, df.dtypes)]
                    self.connection.command(
                        f"CREATE TABLE {self.name} ({', '.join(keys)}) ENGINE {self.engine} Order By {self.orderby}")
                self.connection.insert_df(self.name, df)
            except StopIteration:
                print("Done!")
                break

        beep()

    def insert_data_alchemy(self):  # needs review
        engine = create_engine(
            'postgresql://postgres:Arbit13243546576879@localhost:5432/postgres')

        while True:
            try:
                df = next(self.dfo)
                print(df)
                df.to_sql(self.name, con=engine, if_exists='append')
            except StopIteration:
                print("Done!")
                break


if __name__ == "__main__":
    n = "api_management_1"
    p = "./data/api_management_1.csv"
    opt = "default"

    o = cc_dump(n, p, engine="MergeTree", orderby="received_at")
    o.init_client(database="api")
    o.df_object("csv")
    # df: pd.DataFrame = next(o.dfo)
    o.insert_data(option=opt, flag="v")
