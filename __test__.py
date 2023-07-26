import pandas as pd
import numpy as np
import random as rn


def r(): return rn.randint(1, 100)


a = {1: {1: r()}, 2: {2: r()}}

df = pd.DataFrame(a)


print(df)
print(df.loc[1])
df[1] = pd.to_datetime(df[1])
print(df.loc[1, 1])
