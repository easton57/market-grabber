import market_helper as mh

from time import sleep
from pathlib import Path


def main():
    # Create conf and log directories if needed
    Path("conf").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)

    # Read symbols from symbols.txt and download the data
    try:
        with open('conf/symbols.txt', 'r') as f:
            symbols = f.read().splitlines()

        if symbols == []:
            raise FileNotFoundError
    except FileNotFoundError:
        print("Error reading symbols! Please enter symbols until satisfied then press enter with a blank line.")
        symbols = []

        with open('conf/symbols.txt', 'a') as f:
            while True:
                line = input("Enter a symbol from yahoo finance: ")

                if line == "":
                    break

                symbols.append(line)

                f.write(f"{line}\n")

    # Request the data
    for i in symbols:
        mh.download(i)
        mh.download(i, interval="1m")

if __name__ == "__main__":
    main()