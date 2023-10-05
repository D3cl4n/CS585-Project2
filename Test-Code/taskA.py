# show the expected output of task A
def main():
    with open("/Users/declan/Desktop/CS585-Project2/datasets/facein.csv", "r") as f:
        for line in f.readlines():
            arr = line.split(",")
            if arr[2] == "Albanian":
                print(arr[1] + arr[4])

if __name__ == '__main__':
    main()