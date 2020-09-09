import pandas as pd
import numpy as np

count = 0
unique = {}
def filter_for(row):
    if row in unique:
        pass
    else:
        unique[row] = [0, 0]
for chunk in pd.read_csv('wapo_ny.csv', sep=',', chunksize=1000000):
    apply_set = chunk['TRANSACTION_CODE']
    apply_set.apply(filter_for)
    count += 1000000
    print('Completed ' + str(round(count/130000, 2)) + '%')
print(unique)
read_unique = pd.DataFrame(unique).T
read_unique.to_csv(path_or_buf='unique_wapo.csv')
