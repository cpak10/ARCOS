import pandas as pd
import numpy as np
 
count = 0
# creating the excel sheet in order to append with columns already in place (step 1)
column_names = [
    'index','REPORTER_DEA_NO', 'REPORTER_BUS_ACT', 'REPORTER_NAME', 'REPORTER_ADDL_CO_INFO', 'REPORTER_ADDRESS1', 'REPORTER_ADDRESS2', 'REPORTER_CITY', 'REPORTER_STATE', 
    'REPORTER_ZIP', 'REPORTER_COUNTY', 'BUYER_DEA_NO', 'BUYER_BUS_ACT', 'BUYER_NAME', 'BUYER_ADDL_CO_INFO', 'BUYER_ADDRESS1', 'BUYER_ADDRESS2', 'BUYER_CITY', 'BUYER_STATE', 
    'BUYER_ZIP', 'BUYER_COUNTY', 'TRANSACTION_CODE', 'DRUG_CODE', 'NDC_NO', 'DRUG_NAME', 'QUANTITY', 'UNIT', 'ACTION_INDICATOR', 'ORDER_FORM_NO', 'CORRECTION_NO', 'STRENGTH', 
    'TRANSACTION_DATE', 'CALC_BASE_WT_IN_GM', 'DOSAGE_UNIT', 'TRANSACTION_ID', 'Product_Name', 'Ingredient_Name', 'Measure', 'MME_Conversion_Factor', 'Combined_Labeler_Name', 
    'Reporter_family', 'dos_str', 'MME'
]
df = pd.DataFrame(columns=column_names)
# make sure to change the file name below to not overwrite an existing database (step 1)
df.to_csv('sell_data.csv')
# comment out step 1 when you are ready to move on, this is just whittling down the larger database for select filters below
state_search = ['NY']
no_dups = ['PRACTITIONER', 'RETAIL PHARMACY', 'CHAIN PHARMACY', 'PRACTITIONER-DW/100', 'PRACTITIONER-DW/30', 'PRACTITIONER-DW/275']
purch_search = ['S']
trash = ['MLP-NURSE PRACTITIONER', 'PRACTITIONER-MILITARY']
# this is the actual process for whittling down, for loop with chunks
for chunk in pd.read_csv('ARCOS_pro.csv', sep='|', chunksize=1000000):
    the_trash = chunk.loc[chunk['BUYER_BUS_ACT'].isin(trash)]
    chunk = chunk.drop(the_trash.index)
    chunk = chunk.loc[chunk['BUYER_BUS_ACT'].isin(no_dups)]
    chunk = chunk.loc[chunk['TRANSACTION_CODE'].isin(purch_search)]
    narrowed = chunk.loc[chunk['BUYER_STATE'].isin(state_search)]
    narrowed.to_csv('sell_data.csv', mode='a', header=False)
    count += 1000000
    print('Completed ' + str(round(count/5000000, 2)) + '%')
