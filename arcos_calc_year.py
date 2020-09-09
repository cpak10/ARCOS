import pandas as pd
import numpy as np
from collections import defaultdict

# calculating mme by year
final_count = defaultdict(float)
update = 0
keyerror_count = 0
attributeerror_count = 0
valueerror_count = 0
def arcos_run(row):
    key, pills = row
    if key in final_count:
        if pills >= 0:
            x, y = final_count.get(key)
            x += pills
            y += 1
            final_count[key] = [x, y]
        else:
            pass
    else:
        if pills >= 0:
            final_count[key] = [pills, 1]
        else:
            pass
state_search = ['NY']
ingre_search = ['HYDROCODONE BITARTRATE HEMIPENTAHYDRATE', 'OXYCODONE HYDROCHLORIDE']
drug_search = ['HYDROCODONE', 'OXYCODONE']
pill_search = ['TAB']
no_dups = ['PRACTITIONER', 'RETAIL PHARMACY', 'CHAIN PHARMACY', 'PRACTITIONER-DW/100', 'PRACTITIONER-DW/30', 'PRACTITIONER-DW/275']
purch_search = ['S']
trash = ['MLP-NURSE PRACTITIONER', 'PRACTITIONER-MILITARY']
for chunk in pd.read_csv('ny_data.csv', sep=',', chunksize=1000000):
    try:
        the_trash = chunk.loc[chunk['BUYER_BUS_ACT'].isin(trash)]
        chunk = chunk.drop(the_trash.index)
        chunk = chunk.loc[chunk['BUYER_BUS_ACT'].isin(no_dups)]
        chunk = chunk.loc[chunk['TRANSACTION_CODE'].isin(purch_search)]
        chunk = chunk.loc[chunk['Measure'].isin(pill_search)]
        # chunk = chunk.loc[chunk['DRUG_NAME'].isin(drug_search)]
        narrowed = chunk.loc[chunk['BUYER_STATE'].isin(state_search)]
        narrowed['cleaned_date'] = pd.to_numeric(narrowed['TRANSACTION_DATE'], errors='coerce').fillna(0)
        pd.to_numeric(narrowed['DOSAGE_UNIT'], errors='coerce').fillna(0)
        narrowed['key'] = narrowed['Combined_Labeler_Name'] + '; ' + narrowed['BUYER_COUNTY'] + '; ' + narrowed['cleaned_date'].astype(str).str[-4:] + '; ' + narrowed['DRUG_NAME']
        apply_set = narrowed[['key', 'DOSAGE_UNIT']]
        apply_set.apply(arcos_run, axis=1)
    except KeyError:
        keyerror_count += 1
    except AttributeError:
        attributeerror_count += 1
    except ValueError:
        valueerror_count += 1
    update += 1000000
    print('Completed ' + str(round(update/200000, 2)) + '%')
    print('Found ' + str(keyerror_count) + ' KeyErrors')
    print('Found ' + str(attributeerror_count) + ' AttributeErrors')
    print('Found ' + str(valueerror_count) + ' ValueErrors')
read_count = pd.DataFrame(final_count).T
read_count.to_csv(path_or_buf='arcos_mme.csv')