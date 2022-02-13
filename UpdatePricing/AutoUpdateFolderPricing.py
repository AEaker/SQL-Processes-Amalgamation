
import sqlalchemy as sa
import pandas as pd
import datetime as dt
import numpy as np
import os

engine = sa.create_engine("mssql+pymssql://username:password@server/databasde", echo=True)

df = {}
i = 1
files = [ f for f in os.listdir('Input') if os.path.isfile('Input/'+ f) ]
print(files)
for filename in files:
    df[i] = pd.read_excel('Input/' + filename, usecols=['upc_code', 'item_id', 'supplier_part_no', 'dist_net', 'supplier_id'],dtype={'upc_code': str})
    i += i

ConCat = pd.concat(df, ignore_index=True)
ConCat['inv_mast_uid'] = ''
ConCat['dist_net'].fillna(0, inplace=True)
ConCat.to_sql('aaron_temp_table', con=engine, if_exists='replace')

GetUID = '''
UPDATE att
SET att.inv_mast_uid = inv_mast.inv_mast_uid
FROM aaron_temp_table AS att
INNER JOIN inv_mast ON (inv_mast.item_id = att.item_id)'''
engine.execute(GetUID)

today = "PRICE UPDATED " + dt.date.today().strftime("%m/%d/%y")
print(today)




UpdateCostByUPC = '''
UPDATE invsup
    SET invsup.cost = att.dist_net,
        invsup.list_price = att.dist_net,
        invsup.catalog_name = %s,
        invsup.primary_supplier_flag  = 'Y'
        FROM inventory_supplier AS invsup
        INNER JOIN aaron_temp_table AS att ON (att.upc_code = invsup.upc_code)
        WHERE invsup.supplier_id = att.supplier_id
        and att.upc_code IS NOT NULL
        ;'''

engine.execute(UpdateCostByUPC,(today))


UpdateCost = '''
UPDATE invsup
    SET invsup.cost = att.dist_net,
        invsup.list_price = att.dist_net,
        invsup.primary_supplier_flag = 'Y',
        invsup.catalog_name = %s
        FROM inventory_supplier AS invsup
        INNER JOIN aaron_temp_table AS att ON (att.inv_mast_uid = invsup.inv_mast_uid)
        WHERE invsup.supplier_id = att.supplier_id
        ;'''


engine.execute(UpdateCost,(today))



Date = dt.date.today().strftime("%m.%d.%y")
FindMissingItems = '''
SELECT * FROM aaron_temp_table
WHERE inv_mast_uid = '' ; '''
MissingQ = pd.read_sql_query(FindMissingItems, engine)
Missing = pd.DataFrame(MissingQ)
Missing.to_excel('Output/MissingItems' + Date + '.xlsx',index=None)
