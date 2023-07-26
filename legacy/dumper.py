import clickhouse_connect
import pandas as pd
# import numpy as np

client = clickhouse_connect.get_client(
    host='localhost', username='default', password='Arbit13243546576879')

table_name = "test"
client.command(f"DROP TABLE {table_name}")
client.command(
    f"CREATE TABLE {table_name} (key Int, value Int, name String) ENGINE MergeTree ORDER BY name")
# # add fn to check if table exists

# df = pd.read_csv("./sample/sample.csv", index_col=0)
df = pd.read_json("./sample/sample.json")
client.insert_df(table_name, df)
