# %%
import pandas as pd
import numpy as np

# %%
df1 = pd.read_csv('dataset1.csv', index_col='invoice_id')
df2 = pd.read_csv('dataset2.csv')

# %%
#join two datasets and get rate
df_combined = pd.merge(df1, df2, how='inner', on='counter_party')

# %%
#Get max rating by counter_party
grouped_counterparty = df_combined.groupby('counter_party')
counterparty_max_rating = pd.DataFrame(grouped_counterparty['rating'].agg(np.max))
counterparty_max_rating = pd.concat([counterparty_max_rating, pd.DataFrame({'rating': 'N/A'}, index=['Total'])])
counterparty_max_rating.reset_index(inplace=True)
counterparty_max_rating = counterparty_max_rating.rename(columns= {'index':'counter_party', 'rating': 'max(rating by counterparty)'})
counterparty_max_rating

# %%
#split into ARAP and ACCR
arap_dataset = df_combined[df_combined['status'] == 'ARAP']
accr_dataset = df_combined[df_combined['status'] == 'ACCR']

# %%
#get group by le records
le_group_by_arap = arap_dataset.groupby('legal_entity')['value'].sum().reset_index().rename(columns={'index': 'legal_entity', 'value': 'sum(value where status = ARAP)'})
le_group_by_accr = accr_dataset.groupby('legal_entity')['value'].sum().reset_index().rename(columns={'index': 'legal_entity', 'value': 'sum(value where status = ACCR)'})
le_group_by = pd.merge(le_group_by_arap, le_group_by_accr, how='inner', on='legal_entity')
le_group_by["counter_party"] = 'Total'
le_group_by["tier"] = 'Total'
le_group_by["max(rating by counterparty)"] = 'N/A'
le_group_by

# %%
#get group by le, cp records
le_cp_group_by_arap = arap_dataset.groupby(['legal_entity', 'counter_party'])['value'].sum().reset_index().rename(columns={'index': 'legal_entity', 'value': 'sum(value where status = ARAP)'})
le_cp_group_by_accr = accr_dataset.groupby(['legal_entity', 'counter_party'])['value'].sum().reset_index().rename(columns={'index': 'legal_entity', 'value': 'sum(value where status = ACCR)'})
le_cp_group_by = pd.merge(le_cp_group_by_arap, le_cp_group_by_accr, how='outer', on=['legal_entity', 'counter_party'])
le_cp_group_by["tier"] = 'Total'
le_cp_group_by = pd.merge(le_cp_group_by, counterparty_max_rating, how='inner', on='counter_party')
le_cp_group_by

# %%
#get group by cp records
cp_group_by_arap = arap_dataset.groupby('counter_party')['value'].sum().reset_index().rename(columns={'index': 'counter_party', 'value': 'sum(value where status = ARAP)'})
cp_group_by_accr = accr_dataset.groupby('counter_party')['value'].sum().reset_index().rename(columns={'index': 'counter_party', 'value': 'sum(value where status = ACCR)'})
cp_group_by = pd.merge(cp_group_by_arap, cp_group_by_accr, how='outer', on='counter_party')
cp_group_by['legal_entity'] = 'Total'
cp_group_by['tier'] = 'Total'
cp_group_by = pd.merge(cp_group_by, counterparty_max_rating, how='inner', on='counter_party')
cp_group_by

# %%
#get group by tier records
ti_group_by_arap = arap_dataset.groupby('tier')['value'].sum().reset_index().rename(columns={'index': 'tier', 'value': 'sum(value where status = ARAP)'})
ti_group_by_accr = accr_dataset.groupby('tier')['value'].sum().reset_index().rename(columns={'index': 'tier', 'value': 'sum(value where status = ACCR)'})
ti_group_by = pd.merge(ti_group_by_arap, ti_group_by_accr, how='outer', on='tier')
ti_group_by['legal_entity'] = 'Total'
ti_group_by['counter_party'] = 'Total'
ti_group_by["max(rating by counterparty)"] = 'N/A'
ti_group_by

# %%
#combine totals
combined_group_by_totals = pd.concat([le_group_by, le_cp_group_by, cp_group_by, ti_group_by], ignore_index=True)
combined_group_by_totals

# %%
#output to file
combined_group_by_totals.to_csv('out_pandas.csv', index=False, columns=['legal_entity', 'counter_party', 'tier', 'max(rating by counterparty)', 'sum(value where status = ARAP)', 'sum(value where status = ACCR)'], na_rep=0.0)


