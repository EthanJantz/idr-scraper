
import pandas as pd
import os
import yaml

path_proj = os.path.realpath('./')

with open(path_proj + r'/ingest_data/config.yaml', 'r') as file:
    ingest_config = yaml.safe_load(file)

ret_sales_year = ingest_config['IDR']['years']['ret_sales']
eav_year = ingest_config['IDR']['years']['eav']

ret_sales_path = path_proj + r'\ingest_data\idr\ret_sales\transform\output\transformed_data.csv'
ret_sales = pd.read_csv(ret_sales_path)
ret_sales = ret_sales[ret_sales['year'] == ret_sales_year]

# The following dictionary connects muni names from the EAV data (left) with the names in the ret sales data (right)
eav_ret_muni_names = {
    'Arlington Hts': 'Arlington Heights',
    'Chicago Hts': 'Chicago Heights',
    'Cicero Twp': 'Cicero',
    'East Hazelcrest': 'East Hazel Crest',
    'Elk Grove': 'Elk Grove Village',
    'Forestview': 'Forest View',
    'Glendale Hts': 'Glendale Heights',
    'Harwood Hts': 'Harwood Heights',
    'Hazelcrest': 'Hazel Crest',
    'Indian Head': 'Indian Head Park',
    'Lincolnwood (Cook)': 'Lincolnwood',
    'Lynnwood': 'Lynwood',
    'Mt Prospect': 'Mount Prospect',
    'No Aurora': 'North Aurora',
    'N Barrington': 'North Barrington',
    'North Lake': 'Northlake',
    'Palos Hts': 'Palos Heights',
    'Prospect Hts': 'Prospect Heights',
    'Round Lake Bch': 'Round Lake Beach',
    'Round Lake Hts': 'Round Lake Heights',
    'South Chicago Hts': 'South Chicago Heights',
    'So Elgin': 'South Elgin',
    'St Charles': 'St. Charles',
    'Wilmington (Will)': 'Wilmington',
    'Cook Co': 'Cook County Government',
    'Will Co': 'Will County Government',
    'Lake Co': 'Lake County Government',
    'McHenry Co': 'McHenry County Government',
    'DuPage Co': 'DuPage County Government',
    'Kane Co': 'Kane County Government',
    'Kane Cty': 'Kane County Government',
    'Kendall Co': 'Kendall County Government'
}

counties = ['Cook Co', 'Will Co', 'Lake Co', 'McHenry Co', 'DuPage Co', 'Kane Co', 'Kendall Co']

# eav_2020_path = r'https://tax.illinois.gov/content/dam/soi/en/web/tax/research/taxstats/propertytaxstatistics/documents/y2020tbl28.xlsx' # This was used to verify the process generates the same output from previous years
eav_path = r'https://tax.illinois.gov/content/dam/soi/en/web/tax/research/taxstats/propertytaxstatistics/documents/y{}tbl28.xlsx'.format(eav_year)
# Two header rows with relevant information, will ultimately want to take EAV values and district names only
eav_data = pd.read_excel(eav_path, header = [2])


# Pull out the value in District ID that identifies if the record is a muni and store in new col
eav_data['is_muni'] = eav_data['Unnamed: 0'].str[6:8] 
# Filter down to only records that are munis and Cicero Twp (24 is the code in the district ID that designates that the record is a municipality)
eav_data = eav_data[(eav_data['is_muni'] == '24') | (eav_data['Unnamed: 1'] == 'Cicero Twp') | (eav_data['Unnamed: 1'].isin(counties))] # Cicero is a township but still needs to be included
# Modify district names using the muni name dictionary from above
eav_data['Unnamed: 1'] = eav_data['Unnamed: 1'].map(lambda val: eav_ret_muni_names[val] if val in eav_ret_muni_names else val)
# Filter down to district names that have a district name in retail sales data
eav_muni_data = eav_data[eav_data['Unnamed: 1'].isin(ret_sales['MUNI'].tolist())]
    
# Omit columns after EAV values (end at column 13), there are Extension values that are irrelevant to this analysis included in table 28
eav_muni_data = eav_muni_data.iloc[:, 0:12].rename(columns = {'Unnamed: 0': 'district_id',
                                                    'Unnamed: 1': 'MUNI',
                                                    'Unnamed: 2': 'year',
                                                    'Total': 'total_eav',
                                                    'Residential': 'resi_eav',
                                                    'Total Farm': 'total_farm_eav',
                                                    'Farm A': 'farm_a_eav',
                                                    'Farm B': 'farm_b_eav',
                                                    'Commercial': 'comm_eav',
                                                    'Industrial': 'ind_eav',
                                                    'Railroad': 'rail_eav',
                                                    'Mineral': 'mineral_eav'})


eav_muni_data.drop(['district_id', 'year'], axis = 1, inplace = True)


eav_muni_summed = eav_muni_data.groupby(['MUNI']).sum().reset_index()


export_path = path_proj + r'\ingest_data\idr\eav\output\eav_data.csv'
eav_muni_summed.to_csv(export_path, index = False)
