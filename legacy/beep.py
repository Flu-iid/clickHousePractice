from time import sleep
from sys import argv


def beep(t: float = 5):
    """
    make beep sound for determined time
    """
    shapes = ["\\", "|", "/", "-"]
    d = 0.03
    n = 0
    while t > 0:
        print(shapes[n % len(shapes)], "\a", end="\r")
        sleep(d)
        t -= d
        n += 1
    return 0


if __name__ == "__main__":
    from sys import argv
    import signal
    help = """
Simple beep command.

python3 beep.py [secondsfloat]

give number of seconds(float) as argument,
for beeping in those given time.
"""
    # signal.signal(signal.SIGSTOP, print("ha"))
    if len(argv) == 1:
        beep(1)
    elif argv[1] in ["h", "-h", "help", "--help"]:
        print(help)
    else:
        try:
            t = float(argv[1])
            beep(t)
        except KeyboardInterrupt:
            print("\nout!")
        except:
            print("> bad input")
            print(help)
