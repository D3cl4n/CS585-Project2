import csv
import datetime
from datetime import date, timedelta, datetime

def main():
    # Get today's date.
    today = datetime.today()

    # Create a dictionary to store the ByWho IDs that have been visited within the last 90 days
    pageIDs = {}

    # Open the CSV file.
    with open("C:/Users/Rakesh/IdeaProjects/CS585-Project2/datasets/access.csv", "r") as access:
        reader = csv.reader(access)

        # Loop through the rows in the CSV file.
        for row in reader:

            # Get the date field from the row.
            date_field = row[4]

            # Convert the date field to a datetime object.
            # datetime_object = datetime.strptime(datetime_str, '%m/%d/%y %H:%M:%S')
            date_field_datetime = datetime.strptime(date_field, '%Y-%m-%d %H:%M:%S')

            # Check if the date field is within 90 days of today.
            if date_field_datetime >= today - timedelta(days=90) and date_field_datetime <= today:

                # Add the ByWho ID to the dictionary if it's not there
                if row[1] not in pageIDs:
                    pageIDs[row[1]] = 1

    # Close the CSV file.
    access.close()

    # Loop through facein.csv and print out the IDs that are not in pageIDs dictionary
    with open("C:/Users/Rakesh/IdeaProjects/CS585-Project2/datasets/facein.csv", "r") as facein:
        reader = csv.reader(facein)

        # Loop through the rows in the CSV file.
        for row in reader:

            if row[0] in pageIDs:
                print(row[0] + ',' + row[1])

    facein.close()

if __name__ == '__main__':
    main()