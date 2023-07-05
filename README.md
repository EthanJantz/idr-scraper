This tool consists of two data collection processes. 

The first scrapes data from the Illinois Department of Revenue's (IDOR) Standard Industrial Classification (SIC) Reporting page and calculates the total taxable retail sales revenue for municipalities and counties. This process is found within the `ret_sales` folder of this repository. 

The second pulls table 28 for a given year from IDOR's tax statistics database, extracts EAV values for the northeast region of Illinois by municipality, and converts it into a machine readable format. This process is found within the `eav` folder of this repository.

This process follows the task-based workflow approach used by the Human Rights Data Analysis Group (HRDAG), albeit in a less mature form. It does not utilize makefiles to run the process. Instead the user must run the tasks in the following order:

1. `/ret_sales/import`
2. `/ret_sales/transform`
3. `/eav`
4. `/export`

The `config.yaml` file describes which year (or years) the process will pull for. Note that there are limitations on pulling historic data due to data availability. EAV data has only been tested from 2019 to present, though table 28 is available in prior years. For retail sales revenue this process will only currently work for years after 1999. 