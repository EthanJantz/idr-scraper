import pandas as pd
import os
import yaml

path_proj = os.path.realpath('./')

with open(path_proj + r'/config.yaml', 'r') as file:
    ingest_config = yaml.safe_load(file)

ret_sales_year = ingest_config['IDR']['years']['ret_sales']
eav_year = ingest_config['IDR']['years']['eav']

eav_path = path_proj + r'\eav\output\eav_data.csv'
ret_sales_path = path_proj + r'\ret_sales\transform\output\transformed_data.csv'

eav_data = pd.read_csv(eav_path)
ret_sales_data = pd.read_csv(ret_sales_path)

# Join data
ret_sales_data = ret_sales_data[ret_sales_data['year'] == ret_sales_year] # Technically the ret_sales process can scrape multiple years of data
merged_table = pd.merge(ret_sales_data, eav_data, on='MUNI')

# Rename columns for later processing
export_col_names = {'MUNI': 'GEOG', 
                    'total_eav': 'TOT_EAV',
                    'resi_eav': 'RES_EAV', 
                    'total_farm_eav': 'FARM_EAV',
                    'comm_eav': 'CMRCL_EAV',
                    'ind_eav': 'IND_EAV',
                    'rail_eav': 'RAIL_EAV',
                    'mineral_eav': 'MIN_EAV',
                    'RET_SALES': 'RET_SALES',
                    'GEN_MERCH': 'GEN_MERCH'}

merged_table.rename(columns = export_col_names,
                    errors = 'raise',
                    inplace = True)
merged_table = merged_table[export_col_names.values()]
merged_table.set_index('GEOG', inplace = True)

# Sum county values to generate region value
region_summary = merged_table[merged_table.index.str.contains('County Government')]
region_summary.loc['Region'] = region_summary.sum()
region_summary = region_summary[region_summary.index == 'Region']
export_table = pd.concat([merged_table, region_summary])

# Rename GEOG values 
geog_rename = {
    'Lake In The Hills': 'Lake in the Hills',
    'Lagrange': 'La Grange',
    'Lagrange Park': 'La Grange Park',
    'Cook County Government': 'Cook County',
    'Will County Government': 'Will County',
    'Lake County Government': 'Lake County',
    'McHenry County Government': 'McHenry County',
    'Dupage County Government': 'DuPage County',
    'Kane County Government': 'Kane County',
    'Kendall County Government': 'Kendall County'
    }
export_table.index = export_table.index.map(lambda val: geog_rename[val] if val in geog_rename else val)

export_path = path_proj + r'\export\output\IDR_{}_{}.csv'.format(eav_year, ret_sales_year)
export_table.to_csv(export_path)
