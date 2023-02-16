First you'll need to Install Google Cloud SDK to authenticate Google Earth Engine
Then you need to install some required libraries for running GEEDaR tools

After that, you already have evertything up to run GEEDaR.

But, what does GEEDaR means?
-> Google Earth Engine Data Retriever (GEEDaR)

Basicly, GEEDaR is intended to retrieve data from Google Earth Engine.
Based on the provided list of sites and dates and on the chosen processing algorithms, 
it retrieves the corresponding satellite data.
Supported products include those from MODIS, VIIRS, Landsat, Sentinel-2 and Sentinel-3.

For execute GEEDaR you will need give him some parameters. Here goes a brief review for them.

There are some operation models that is worth to explain, so let's start.

Mode 1 - Specific dates
Use when you want to pair satelits data and field data "dados de campo". This mode requires as input a csv file
that contains some columns: id, date, lat and long. 

Note: The input data can contain other columns, GEEDaR will not ready data out of columns out of mentioned names.

Mode 2 - Date range
Use when you want to generat time series for satelite data. This mode also requires a csv file as input,
in this case whith these columns names: id, lat, long, start_date and end_date.

...keep writing