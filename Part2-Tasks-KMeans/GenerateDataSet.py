import csv
import random

# Generate the dataset of points for our KMeans Algorithm
def main():
    with open("KMeansDataset.csv", 'w', newline='') as file:
        writer = csv.writer(file)
        # Generate 5000 data points
        for n in range(5000):
            XValue = random.randint(0,10000)
            YValue = random.randint(0,10000)
            writer.writerow([XValue,YValue])

if __name__ == '__main__':
    main()