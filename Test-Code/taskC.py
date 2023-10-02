# show the expected output of task C
from itertools import count
import re


def main():
    with open("/Users/declan/Desktop/CS585-Project2/datasets/facein.csv", "r") as f:
        country_codes = {}
        for line in f.readlines():
            arr = line.split(",")
            code = arr[3]
            ID = arr[0]

            if code in country_codes.keys():
                country_codes[code] = country_codes[code] + 1
            
            elif code not in country_codes.keys():
                country_codes.update({code : 1})

    print(country_codes)

if __name__ == '__main__':
    main()