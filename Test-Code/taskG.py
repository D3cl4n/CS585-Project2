import datetime
from datetime import date, timedelta, datetime

def main():
    curr_date = datetime.today()
    ids = []

    access_path = "/Users/declan/Desktop/CS585-Project2/datasets/access.csv"
    facein_path = "/Users/declan/Desktop/CS585-Project2/datasets/facein.csv"

    with open(access_path, "r") as f:
        for line in f.readlines():
            arr = line.split(",")
            date = arr[4].strip("\n")
            date_formatted = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

            if curr_date - timedelta(days=90) <= date_formatted <= curr_date:
                id_field = arr[1]
                if id_field not in ids:
                    ids.append(id_field)
        
    f.close()

    with open(facein_path, "r") as f:
        for line in f.readlines():
            arr = line.split(",")
            id_field = arr[0]
            
            if id_field not in ids:
                name_field = arr[1]
                print(id_field + "," + name_field)
                
    f.close()

if __name__ == '__main__':
    main()