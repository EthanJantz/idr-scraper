from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from itertools import product
import yaml

path_proj = os.path.realpath('./')

with open(path_proj + r'/config.yaml', 'r') as file:
    ingest_config = yaml.safe_load(file)

MUNI_CNTY_GOV_FIELD = '0000000'
year = ingest_config['IDR']['years']['ret_sales']
report_period_field = ['{}{}'.format(y, i) for y in year for i in [0,1,2,3,4]] # 0 is calendar year, quarters are 1 - 4; for example Q1 2021 is '20211'
TAX_TYPE_FIELD = '00'
county_name_field = {
    'Cook': '016',
    'McHenry': '056',
    'DuPage': '022',
    'Kane': '045',
    'Kendall': '047',
    'Lake': '049',
    'Will': '099'
}

out_table = pd.DataFrame()

def htmlToList(contents):
    '''
    This is used to convert the soup contents for MT and ST columns into lists.
    You can use this to parse any other columns as long as you identify the correct location in the parsed HTML.
    Takes a soup.contents as input
    '''
    contents_comp = [i for i in contents if i != soup.br][2:]

    raw = [i.replace('\n', '0').replace(',', '') for i in contents_comp]

    cleaned_string = ['' if x == '0' else x.lstrip('0') for x in raw]

    return(cleaned_string)

for county, period in product(county_name_field, report_period_field):
    print(county + period)
    url = 'https://www.revenue.state.il.us/app/kob/KOBReport?r=Specific&p={}&m={}&c={}&t={}'.format(period, MUNI_CNTY_GOV_FIELD, county_name_field[county], TAX_TYPE_FIELD)        
    data = requests.get(url)
    soup = BeautifulSoup(data.text, 'html.parser')
    
    body_tables = soup.body.find_all('table', recursive=False)
    data_table = body_tables[1].find_all('tr', recursive=False)[0].find_all('td', recursive=False)
    muni_tables = data_table[0].find_all('table', recursive=False)
    muni_tables = muni_tables[:-1]
    assert len(muni_tables) != 0, "muni_tables should not be empty"

    for table in muni_tables:
        city_name = table.select_one('tr:first-child td:first-child font').string
        county_name = table.select_one('tr:first-child td:nth-child(2) font').string
        num_taxpayers = table.select_one('tr:first-child td:nth-child(3) font').string.replace('Number of Taxpayers:', '').replace(' ', '')
        period = table.select_one('tr:nth-child(2) td i').string

        categories_contents = table.select_one('tr:nth-child(3) td:first-child td:nth-child(3)').contents
        categories = [i for i in categories_contents if i != soup.br]
        categories.append('Total')

        # print('City: ' + city_name)
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            # print(cols)
            for col in cols:
                try:
                    header = col.select_one('center').string

                    if header == 'ST':
                        st_out = htmlToList(col)
                        # print(st_out)
                        # print('ST found')
                    if header == 'MT':
                        mt_out = htmlToList(col)
                        # print(mt_out)
                        # print('MT found')
                    if header == 'CT':
                        ct_out = htmlToList(col)
                        # print(ct_out)
                        # print('CT found')
                except: 
                    pass
                
                
        # CT is not going to be defined for most records.
        # This ensures it is placed for each table.
        if 'ct_out' not in locals():
            ct_out = [''] * 11  
            
        # print('Output:')
        # print(st_out)
        # print(mt_out)
        # print(ct_out)
        out_df = pd.DataFrame({
            'city' : city_name,
            'county' : county_name,
            'period' : period,
            'taxpayers' : num_taxpayers,
            'categories' : categories,
            'st' : st_out,
            'mt' : mt_out,
            'ct' : ct_out
            })

        out_table = out_table.append(out_df)
        
        # Prevent duplicate values from showing up 
        st_out = [''] * 11
        mt_out = [''] * 11
        ct_out = [''] * 11
        # time.sleep(2)

export_path = path_proj + r'\ret_sales\import\output\scraped_data.csv'
out_table.to_csv(export_path, index = False)

# # Testing case, Lake county
# url = 'https://www.revenue.state.il.us/app/kob/KOBReport?r=Specific&p={}&m={}&c={}&t={}'.format(20210, MUNI_CNTY_GOV_FIELD, county_name_field['Lake'], TAX_TYPE_FIELD)
# data = requests.get(url)
# soup = BeautifulSoup(data.text, 'html.parser')

# body_tables = soup.body.find_all('table', recursive=False)
# data_table = body_tables[1].find_all('tr', recursive=False)[0].find_all('td', recursive=False)
# muni_tables = data_table[0].find_all('table', recursive=False)
# muni_tables = muni_tables[:-1]

# table = muni_tables[0]

# city_name = table.select_one('tr:first-child td:first-child font').string
# county_name = table.select_one('tr:first-child td:nth-child(2) font').string
# period = table.select_one('tr:nth-child(2) td i').string
# num_taxpayers = table.select_one('tr:first-child td:nth-child(3) font').string.replace('Number of Taxpayers:', '').replace(' ', '')

# categories_contents = table.select_one('tr:nth-child(3) td:first-child td:nth-child(3)').contents
# categories = [i for i in categories_contents if i != soup.br]
# categories.append('Total')

# rows = table.find_all('tr')
# for row in rows:
#     cols = row.find_all('td')
#     # print(cols)
#     for col in cols:
#         try:
#             header = col.select_one('center').string
#             if header == 'ST':
#                 st_out = htmlToFloatList(col)
#                 # print(st_out)
#             elif header == 'MT':
#                 mt_out = htmlToFloatList(col)
#                 # print(mt_out)
#         except: 
#             pass

# out_df = pd.DataFrame({
#     'city' : city_name,
#     'county' : county_name,
#     'period' : period,
#     'categories' : categories,
#     'st' : st_out,
#     'mt' : mt_out
#     })

# export = export.append(out_df)