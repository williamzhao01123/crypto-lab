#------------------------------------------------------#
# EIP-1559 Related Data
#------------------------------------------------------#
# Note for file eth.db
# William Zhao Sep21
#------------------------------------------------------#

The eth.db file is a SQL database that contains mempool, blockchain, and observed block timestamp data for the period around EIP-1559 (from ~7.25 - ~8.15, depending on exact data files).

There are seven data tables in the database. The names are TR_full, left_merged_TRdata, TR_mem_data, TRblock_data, blockchain_data, block_data, left_merged_data. The content of each data table is exlained below.

# blockchain_data
# A raw data file queried directly from Google BigQuery (table name crypto_ethereum.transactions). It includes relevant information on the transaction level on blockchain like transaction hash, gas price paid per gas, max fee specified by user etc.

# block_data
# A raw data file queried directly from Google BigQuery (table name crypto_ethereum.blocks). It includes relevant information on the block level on blockchain like block size, difficulty, miner, uncle, base fee etc.

# TR_mem_data
# The mempool data collected by the Triangle server from 7.25 to 8.15. It is processed in a way that only the first observation on the mempool is stored in this data table, and transaction hash is used as key. The variable timeSeen in the table, which represents the time when the transaction is first posted on mempool, is essential and is unique in our data.

# TRblock_data
# The block data collected by the Triangle server from 7.25 to 8.15 (8.2 - 8.4 missing). It includes a timestamp for each block, which marks the time that the server observes the block. It differs from miner timestamp, and it will be used to compute waiting time for our purpose.

# left_merged_TRdata
# A derived table by left merging blockchain_data and TR_mem_data. This table includes information of each transaction from BigQuery blockchain and our mempool.

# TR_full
# A derived table by left merging left_merged_TRdata and TRblock_data on block number. This table includes the waiting time computed by the gap between timeSeen in left_merged_TRdata and the block_timestamp in TRblock_data. This is the final data good for use.

# left_merged_data
# A data combining mempool data from four servers. Not ready for use.