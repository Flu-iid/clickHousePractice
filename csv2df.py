import pandas as pd


name = "api_management_2"
path = f"./data/{name}.csv"


def has_duplicated(l: list):
    s = set(l)
    if len(s) == len(l):
        return False
    return True


def dfo(path: str, chunksize: int = 10000):
    """
    DataFrame iterator Object
    """
    return iter(pd.read_csv(path, chunksize=chunksize))


# if __name__ == "__main__":
