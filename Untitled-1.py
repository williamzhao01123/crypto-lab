import pandas as pd
import sqlite3 as sqlite
  
#--------------------------------------------#
# Read and Process the files
#--------------------------------------------#
for day in range(1,15):
    for hour in range(0, 24):
        timestring = str(2021080000 + 100*day + hour)
        LAaddress = "/home/williamzhao/LA/LA" + timestring + ".csv"
        GMaddress = "/home/williamzhao/GM/GM" + timestring + ".csv"
        TRaddress = "/home/williamzhao/TR/TR" + timestring + ".csv"
        MTaddress = "/home/williamzhao/MT/MT" + timestring + ".csv"                        
        try:
            LAdata = pd.read_csv(LAaddress, index_col="Unnamed: 0")
            LAdata["location"] = "LA"
        except:
            LAdata = pd.DataFrame()
        try:
            GMdata = pd.read_csv(GMaddress, index_col="Unnamed: 0")
            GMdata["location"] = "GM"
        except:
            GMdata = pd.DataFrame()
        try:
            TRdata = pd.read_csv(TRaddress, index_col="Unnamed: 0")
            TRdata["location"] = "TR"
        except:
            TRdata = pd.DataFrame()            
        try:
            MTdata = pd.read_csv(MTaddress, index_col="Unnamed: 0")
            MTdata["location"] = "MT"
        except:
            MTdata = pd.DataFrame()

        data = LAdata.append(GMdata).append(TRdata).append(MTdata)
        data = data.groupby(['tx_hash']).apply(lambda x : x.sort_values(by = 'timeSeen', ascending = True).head(1).reset_index(drop = True)).reset_index(drop = True)
        
        outputpath = "/home/williamzhao/ALL/ALL" + timestring + ".csv"
        data = data.to_csv(outputpath)


#--------------------------------------------#
# Write Data into SQL
#--------------------------------------------#
DB_CONNECTOR = sqlite.connect("/home/williamzhao/eth.db")
DB_CURSOR = DB_CONNECTOR.cursor()

DB_CURSOR.execute(
"""
CREATE TABLE IF NOT EXISTS mem_data(
    tr_hash TEXT PRIMARY KEY,
    timeSeen INTEGER,
    gasPrice REAL,
    gasLimit REAL,
    value REAL,
    location TEXT
)
"""
)

DB_CONNECTOR.commit()

for idx, row in data.iterrows():
    DB_CURSOR.execute(
    """
    INSERT INTO mem_data (tr_hash,timeSeen,gasPrice,gasLimit,value)
    SELECT ?,?,?,?,?,?,?,?,?,?,?
    WHERE NOT EXISTS (SELECT * FROM mem_data WHERE tx_hash = ?)
    """
    ,(row[0], row[1], row[2], row[3], row[4], row[5])
    )
DB_CONNECTOR.commit()