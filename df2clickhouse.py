import csv2df
import json2df
import pandas as pd
import numpy as np
import clickhouse_connect as cc

dtype2clickhouse = {"object": "String",
                    np.dtype('O'): "String",
                    "int64": "Int",
                    np.dtype("int64"): "Int",
                    "float64": "Float64",
                    np.dtype("float64"): "Float64",
                    #  "bool":"",
                    "datetime64": "DateTime64",
                    }


client = cc.get_client(
    host="localhost",
    port=8123,
    username="default",
    password="Arbit13243546576879")


name = csv2df.name
path = csv2df.path
dfo = csv2df.dfo(path)

# df: pd.DataFrame = pd.DataFrame(next(csv2df.dfo(path, 100)))
# print(df.duplicated())
while True:
    try:
        df: pd.DataFrame = next(dfo)
        # clean commands here
        # df.dropna(inplace=True)
        df.fillna(0, inplace=True)
        ##
        print(df)  # add verbose flag
        if not client.command(f"EXISTS {name}"):
            keys = [f"{i} {dtype2clickhouse[j]}" for i,
                    j in zip(df.columns.values, df.dtypes)]
            client.command(
                f"CREATE TABLE {name} ({', '.join(keys)}) ENGINE MergeTree Order By ip")
        client.insert_df(name, df)
    except StopIteration:
        print("Done!")
        break

[print("\a") for i in range(15)]
