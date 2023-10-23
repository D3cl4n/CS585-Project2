import csv
def main():

    # Create a dictionary to store the occurrences of each field value.
    occurrences = {}

    # Open the CSV file.
    with open("C:/Users/Rakesh/IdeaProjects/CS585-Project2/datasets/access.csv", "r") as access:
        reader = csv.reader(access)

        # Loop through the rows in the CSV file.
        for row in reader:

            # Get the field value that you want to count the occurrences of.
            field_value = row[2]

            # If the field value is not already in the dictionary, add it with a count of 1.
            if field_value not in occurrences:
                occurrences[field_value] = 1

            # Otherwise, increment the count for the field value.
            else:
                occurrences[field_value] += 1

    # Close the CSV file.
    access.close()

    # Sort the dictionary by the count of each field value, in descending order.
    sorted_occurrences = sorted(occurrences.items(), key=lambda item: item[1], reverse=True)

    # Print the 10 most popular occurrences of the field value.
    mostPopularPages = [0] * 10
    # print("The 10 most popular occurrences of the field value are:")
    for i in range(10):
        # print(f"{i + 1}: {sorted_occurrences[i][0]} ({sorted_occurrences[i][1]})")
        mostPopularPages[i] = sorted_occurrences[i][0]

    # Need to loop through facein.csv and print the ID, Name, and Nationality now of the most popular pages
    with open("C:/Users/Rakesh/IdeaProjects/CS585-Project2/datasets/facein.csv", "r") as facein:
        reader = csv.reader(facein)

        for row in reader:
            pageID = row[0]
            if pageID in mostPopularPages:
                print(row[0] + "," + row[1] + "," + row[2])

    facein.close()

if __name__ == '__main__':
    main()
