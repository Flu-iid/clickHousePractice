# import numpy as np
# import pandas as pd
# import psycopg2
from sqlalchemy import create_engine
import csv2df

name = csv2df.name
path = csv2df.path
dfo = csv2df.dfo(path)


engine = create_engine(
    'postgresql://postgres:Arbit13243546576879@localhost:5432/postgres')


while True:
    try:
        df = next(dfo)
        print(df)
        df.to_sql(name, con=engine, if_exists='append')
    except StopIteration:
        print("Done!")
        break


if __name__ == "__main__":
    [print("\a") for i in range(10)]
