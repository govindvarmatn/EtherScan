import pandas as pd
from lib.SingleStoreConn import SingleStoreConn
import sys

args = sys.argv
if len(args) > 1:
    app_name = sys.argv[1]
else:
    app_name = 'EtherScan'

print('fetching data from excel')
df = pd.read_excel('./Lookup/Lookup.xlsx').astype(str)

print('processing excel')
for col in df.columns:
    df[col] = df[col].str.strip()

print('creating SingleStore connection object')
SingleStoreConn_obj = SingleStoreConn(app_name)
print('inserting data to SingleStore object')
SingleStoreConn_obj.insert_data(df)
print(f'completed inserting {df.shape[0]} records to SingleStore DB')