import pandas as pd
import dask.dataframe as dd
import clickhouse_connect as cc
import numpy as np

name1 = "api_management_1"
path1 = f"./data/{name1}.csv"
name2 = "api_management_2"
path2 = f"./data/api_management_2.csv"


dtype2clickhouse: dict = {"object": "String",
                          np.dtype('O'): "String",
                          "int64": "Int",
                          np.dtype("int64"): "Int",
                          "float64": "Float64",
                          #  "bool":"",
                          "datetime64": "DateTime64",
                          }

# with open(name1+".csv", "r") as fin:
#     count = 0
#     for l in fin.readlines():
#         count += 1
#     print(count)


def has_duplicated(l: list):
    s = set(l)
    if len(s) == len(l):
        return False
    return True


client = cc.get_client(
    host="localhost",
    port=8123,
    username="default",
    password="Arbit13243546576879")

dfo = iter(pd.read_csv(path1, chunksize=10000))
while True:
    try:
        df: pd.DataFrame = next(dfo)
        df.fillna("NNN", inplace=True)
        print(df)
        if not client.command(f"EXISTS {name1}"):
            keys = [f"{i} {dtype2clickhouse[j]}" for i,
                    j in zip(df.columns.values, df.dtypes)]
            print(keys)
            client.command(
                f"CREATE TABLE {name1} ({', '.join(keys)}) ENGINE MergeTree Order By ip")
        client.insert_df(name1, df)
    except StopIteration:
        break
print("DONE!")


# print(df,
#       type(df),
#       df.columns.values, df.columns.value_counts(),
#       "has duplicated : ", has_duplicated(l), type(dfo)
#       )
