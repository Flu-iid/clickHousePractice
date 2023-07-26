def new_line(path: str, destination: str):
    with open(path) as fin:
        with open(destination, "a+") as fout:
            for c in fin.read():
                fout.write(c)
                if c == "}":
                    fout.write("\n")


if __name__ == "__main__":
    path = "./data/json_sample.json"
