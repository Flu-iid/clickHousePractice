import pandas as pd
import numpy as np
import clickhouse_connect as cc

username = "default"
password = "Arbit13243546576879"
host = "localhost"
port = "8123"
db = "api"
main_table = "api.api_management_1_cleanse_2"

client = cc.get_client(username=username,
                       password=password,
                       host=host,
                       port=port,
                       database=db)

tmp_flag = False
for _ in client.command("SELECT * FROM system.databases"):
    if _.strip() == db:
        tmp_flag = True
if not tmp_flag:
    print(db, "not existed")
    print("CREATED ", db, "DATABASE")
    client.command(f"CREATE DATABASE {db}")
client = cc.get_client(
    host=host, port=port, username=username, password=password, database=db)
print("connected to > ", db)

# 3 best services
t = "extra_3_best_services"
client.command(f"CREATE TABLE {t} AS\
                (SELECT DISTINCT  (jy, jm) AS jt FROM {main_table}\
                WHERE jt BETWEEN (1402, 1) AND (1402, 5)\
                ORDER BY jm DESC\
                LIMIT 3) ENGINE MergeTree ORDER BY jt")
