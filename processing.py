import sqlite3 as sqlite
import pandas as pd
import csv


DB_CONNECTOR = sqlite.connect("/home/luyao/ethmempool.db")
DB_CURSOR = DB_CONNECTOR.cursor()

#----------------------------------------#
# Build mirror of eth_data
#----------------------------------------#
DB_CURSOR.execute(
  """
  DROP TABLE mem_data
  """
)
DB_CONNECTOR.commit()

Processing blockchain and mempool data
DB_CURSOR.execute(
   """
    CREATE TABLE IF NOT EXISTS
      mem_data
    AS SELECT
      *
    FROM
      eth_data
  """)
DB_CONNECTOR.commit()

DB_CURSOR.execute(
  """
    UPDATE bc_data
      SET 
      tr_time = STRFTIME(tr_time)
  """)

DB_CURSOR.execute(
   """
    UPDATE mem_data
      SET
      tr_time = STRFTIME(tr_time)
    """
)
DB_CONNECTOR.commit()

#----------------------------------------#
# Build left merge table
#----------------------------------------#
DB_CURSOR.execute(
  """
  DROP TABLE left_merged_data
  """
)
DB_CONNECTOR.commit()

DB_CURSOR.execute(
    """
    CREATE TABLE
      left_merged_data
    AS SELECT 
      b.tr_hash,
      b.value,
      b.tr_time,
      b.block_number,
      b.receipt_gas_used,
      b.gas,
      b.gas_price/POW(10, 9),
      m.tr_time AS 'tr_time_mem',
      86400*(JULIANDAY(b.tr_time) - JULIANDAY(m.tr_time)) AS 'waiting_time'
    FROM 
      bc_data b
    LEFT JOIN mem_data m
    ON
      b.tr_hash = m.tr_hash
    WHERE 
      b.tr_time > STRFTIME('2021-07-14 00:00:00+00')
      AND
      b.tr_time < STRFTIME('2021-07-19 00:00:00+00')
    """
)
DB_CONNECTOR.commit()

#----------------------------------------#
# Build right merge table
#----------------------------------------#
DB_CURSOR.execute(
  """
  DROP TABLE right_merged_data
  """
)
DB_CONNECTOR.commit()

DB_CURSOR.execute(
    """
    CREATE TABLE
      right_merged_data
    AS SELECT 
      m.tr_hash,
      m.value,
      m.tr_time,
      m.gaslimit,
      m.gasprice,
      b.tr_time AS 'tr_time_bc',
      86400*(JULIANDAY(b.tr_time) - JULIANDAY(m.tr_time)) AS 'waiting_time'
    FROM 
      mem_data m
    LEFT JOIN bc_data b
    ON
      m.tr_hash = b.tr_hash
    WHERE 
      m.tr_time > STRFTIME('2021-07-14 00:00:00+00')
      AND
      m.tr_time < STRFTIME('2021-07-19 00:00:00+00')
    """
)
DB_CONNECTOR.commit()