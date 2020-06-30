import pandas as pd
import numpy as np
from collections import defaultdict

# calculating mme
# final_count = defaultdict(float)
# update = 0
# keyerror_count = 0
# attributeerror_count = 0
# valueerror_count = 0
# def arcos_run(row):
#     key, conv, gram = row
#     MME = float(conv) * float(gram) * 1000
#     if key in final_count:
#         final_count[key] += MME
#     else:
#         final_count[key] = MME
# for chunk in pd.read_csv('arcos_all.tsv', sep='\t', chunksize=1000000):
#     try:
#         pd.to_numeric(chunk['MME_Conversion_Factor'], errors='coerce').fillna(0)
#         pd.to_numeric(chunk['CALC_BASE_WT_IN_GM'], errors='coerce').fillna(0)
#         chunk['key'] = chunk['BUYER_NAME'] + '; ' + chunk['BUYER_BUS_ACT']
#         apply_set = chunk[['key', 'MME_Conversion_Factor', 'CALC_BASE_WT_IN_GM']]
#         apply_set.apply(arcos_run, axis=1)
#     except KeyError:
#         keyerror_count += 1
#     except AttributeError:
#         attributeerror_count += 1
#     except ValueError:
#         valueerror_count += 1
#     update += 1000000
#     print('Completed ' + str(round(update/3810000, 2)) + '%')
#     print('Found ' + str(keyerror_count) + ' KeyErrors')
#     print('Found ' + str(attributeerror_count) + ' AttributeErrors')
#     print('Found ' + str(valueerror_count) + ' ValueErrors')
# read_count = (pd.DataFrame(final_count, index=[0])).T
# read_count.to_csv(path_or_buf='arcos_mme.csv')

# calculating pills
final_count = defaultdict(float)
update = 0
keyerror_count = 0
attributeerror_count = 0
valueerror_count = 0
def arcos_run(row):
    key, pill = row
    PILLS = float(pill)
    if key in final_count and PILLS > 0:
        final_count[key] += PILLS
    if key in final_count and PILLS > 0:
        final_count[key] += PILLS
    else:
        final_count[key] = PILLS
for chunk in pd.read_csv('arcos_all.tsv', sep='\t', chunksize=1000000):
    try:
        pd.to_numeric(chunk['DOSAGE_UNIT'], errors='coerce').fillna(0)
        apply_set = chunk[['Combined_Labeler_Name', 'DOSAGE_UNIT']]
        apply_set.apply(arcos_run, axis=1)
    except KeyError:
        keyerror_count += 1
    except AttributeError:
        attributeerror_count += 1
    except ValueError:
        valueerror_count += 1
    update += 1000000
    print('Completed ' + str(round(update/3810000, 2)) + '%')
    print('Found ' + str(keyerror_count) + ' KeyErrors')
    print('Found ' + str(attributeerror_count) + ' AttributeErrors')
    print('Found ' + str(valueerror_count) + ' ValueErrors')
read_count = (pd.DataFrame(final_count, index=[0])).T
read_count.to_csv(path_or_buf='arcos_mme.csv')