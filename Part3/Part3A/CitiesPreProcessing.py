import pandas as pd

df = pd.read_csv('uscities.csv')
# print(pd.options.display.max_rows)
LatAndLong = df[['lat','lng']]
# print(LatAndLong)
LatAndLong.to_csv('uscities_LatAndLong.csv',header=False,index=False)

# Find the min and max of both the lat and long columns
print('Maximum Latitude and Longitude:')
print(LatAndLong.max())
# Maximum Latitude and Longitude:
# lat     71.2727
# lng    174.1110
print('Minimum Latitude and Longitude:')
print(LatAndLong.min())
# Minimum Latitude and Longitude:
# lat     17.9559
# lng   -176.6295