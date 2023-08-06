# import clickhouse_connect as cc

# database = "api"

# c = cc.get_client(username="default",
#                   password="Arbit13243546576879")

# for _ in c.command("SELECT * FROM system.databases"):
#     if _.strip() == database:
#         print("ok")
#         c = cc.get_client(username="default",
#                           password="Arbit13243546576879", database=database)

# print(c.database)
# c.insert(column_names=["code"], data=[None])
# c.command(f"INSERT INTO api.api_management_1 (code) VALUES ()")

a = {1, 2, 3}
b = {4, 2, 5}

print(a.intersection(b))
