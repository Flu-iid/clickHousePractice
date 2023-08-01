# import clickhouse_connect as cc


# c = cc.get_client(username="default",
#                   password="Arbit13243546576879")

# for _ in c.command("SELECT * FROM system.databases"):
#     if _.strip() == "default":
#         print("ok")
#         c = cc.get_client(username="default",
#                           password="Arbit13243546576879", database="default")

# print(c.database)
s1 = {1, 2}
s2 = {3, 4}
print(s1.union(s2))
