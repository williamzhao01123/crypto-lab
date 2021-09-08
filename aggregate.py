import sqlite3 as sqlite
import pandas as pd
import csv
# 5th Percentile
def q05(x):
    return x.quantile(0.05)

# 95th Percentile
def q25(x):
    return x.quantile(0.25)

# 50th Percentile
def q50(x):
    return x.quantile(0.5)

# 95th Percentile
def q75(x):
    return x.quantile(0.75)

# 95th Percentile
def q95(x):
    return x.quantile(0.95)

DB_CONNECTOR = sqlite.connect("/home/luyao/ethmempool.db")
DB_CURSOR = DB_CONNECTOR.cursor()

#-----------------------------------------------#
# Left Aggregates to Block Number
#-----------------------------------------------#
DB_CURSOR.execute(
    """
    SELECT 
      block_number,
      gas_price,
      max_fee,
      max_priority_fee,
      waiting_time
    FROM 
      TR_full
    WHERE block_number > 12909999
      AND block_number < 13020000
    """
)

data = DB_CURSOR.fetchall()
output = pd.DataFrame(data)

output.columns = ['block_number', 'gas_price', 'max_fee', 'max_priority_fee', 'waiting_time']
output['gasprice'] = pd.to_numeric(output['gasprice'], errors='coerce')
output['waiting_time'] = pd.to_numeric(output['waiting_time'], errors='coerce')
output['_waiting_time'] = output['waiting_time'].fillna(0)
summarized = output.groupby(['block_number']).agg({'waiting_time': [q05, q25, q50, q75, q95, 'count'], \
                                                   '_waiting_time': [q05, q25, q50, q75, q95, 'count'], \
                                                   'gasprice': [q05, q25, q50, q75, q95, 'count'], \
                                                   'max_fee': [q05, q25, q50, q75, q95, 'count']})
summarized.to_csv("/home/williamzhao/crypto-lab.csv")