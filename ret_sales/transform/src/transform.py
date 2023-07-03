import os
import pandas as pd

project_path = os.path.realpath('./') # /community-data-snapshots

scraped_data = pd.read_csv(project_path + r'\ret_sales\import\output\scraped_data.csv')

# Combine ct and mt columns
scraped_data['mt_ct_combined'] = scraped_data['mt'].fillna(scraped_data['ct'])

# Divide by the tax rate values to determine total amount of taxable revenue
scraped_data['st_rev'] = scraped_data['st'] / .05
scraped_data['mt_ct_rev'] = scraped_data['mt_ct_combined'] / .01

# Remove full calendar year values
scraped_data = scraped_data[~scraped_data['period'].str.contains('Calendar')]
# Select only total revenue values
scraped_data = scraped_data[scraped_data['categories'].str.contains('Total')]

# Pull out year values
scraped_data['year'] = scraped_data['period'].str.extract('(\d{4})', expand=False)

# Remove county identifiers from cities that have them to allow proper grouping (ex. Aurora is split between three counties.)
scraped_data['city'] = scraped_data['city'].str.replace(r"\s*\([^()]*\)", "", regex=True)

# Generate county totals
## "X County Government" values are the values for unincorporated territory within the county
## By summing everything using the "county" column we can identify the actual totals for 
## the counties. 
county_totals = scraped_data.groupby('county').agg({'year': 'first', 'taxpayers': 'sum',
                                    'st': 'sum', 'mt_ct_combined': 'sum',
                                    'st_rev': 'sum', 'mt_ct_rev': 'sum'})

# Recreate city column values from group index
county_totals['city'] = county_totals.index + ' Government'
county_totals.set_index('city', inplace = True)

# Sum quarter values of mt_ct_combined grouped by muni
transformed_data = scraped_data.groupby(['city', 'year']).agg({'city': 'first', 'year': 'first', 'taxpayers': 'first',
                                                               'st': 'sum', 'mt_ct_combined': 'sum', 
                                                               'st_rev': 'sum', 'mt_ct_rev': 'sum'})

transformed_data.set_index('city', inplace = True)

transformed_data = transformed_data[~transformed_data.index.str.contains('County Government')]

export_data = pd.concat([county_totals, transformed_data])
export_data.index.rename('MUNI', inplace = True)

# Rename columns to align with how they're displayed in CDS
## NOTE: mt_ct_rev is not exactly equal to values from previous CDS releases. The difference is generally less than .01%, so they're accurate enough for now.
export_data = export_data.rename(columns = {'st_rev': 'GEN_MERCH', 'mt_ct_rev': 'RET_SALES'})

# Export
export_path = project_path + r'\ret_sales\transform\output\transformed_data.csv'
export_data.to_csv(export_path)