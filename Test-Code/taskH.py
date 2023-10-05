import math

# test code for getting the expected output for task H
def main():
    total_relationships = 0
    user_relationships = {}

    with open("/Users/declan/Desktop/CS585-Project2/datasets/associates.csv", "r") as f:
        for line in f.readlines():
            arr = line.split(",")
            personA = arr[1]
            personB = arr[2]

            total_relationships += 2

            if personA in user_relationships.keys():
                user_relationships[personA] = user_relationships[personA] + 1
            
            elif personA not in user_relationships.keys():
                user_relationships.update({personA : 1})

            elif personB in user_relationships.keys():
                user_relationships[personB] = user_relationships[personB] + 1
            
            elif personB not in user_relationships.keys():
                user_relationships.update({personB : 1})

    average_relationships = math.floor(total_relationships / len(user_relationships))
    print("Users with more than average relationships")
    for k in user_relationships.keys():
        if user_relationships[k] > average_relationships:
            print(f"User ID: {k}, User Relationships: {user_relationships[k]}")

if __name__ == '__main__':
    main()