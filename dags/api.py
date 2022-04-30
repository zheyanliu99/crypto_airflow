# %% get data
import requests
import pandas as pd

# stock
# r = requests.get('https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/2020-10-14?adjusted=true&apiKey=2gEOah0DjDSI9ImMCR_0BL5TSJkvlC4z')

# crypto
date = '2022-04-21'
url = 'https://api.polygon.io/v2/aggs/grouped/locale/global/market/crypto/{}?adjusted=true&apiKey=2gEOah0DjDSI9ImMCR_0BL5TSJkvlC4z'.format(date)
r = requests.get(url)
data = r.json()
data
# %% transform data
data['results']
exchange_symbol = []
close_price = []
highest_price = []
lowest_price = []
transactions = []
weighted_average = []

for cr in data['results']:
    # print(cr)
    exchange_symbol.append(cr['T'])
    close_price.append(cr['c'])
    highest_price.append(cr['h'])
    lowest_price.append(cr['l'])
    transactions.append(cr['n'])
    if 'vw' in cr:
        weighted_average.append(cr['vw'])
    else:
        weighted_average.append(-999)

cr_dict = {
    'exchange_symbol':exchange_symbol,
    'close_price':close_price,
    'highest_price':highest_price,
    'lowest_price':lowest_price,
    'transactions':transactions,
    'weighted_average':weighted_average}

cr_df = pd.DataFrame(cr_dict)

# %%
cr_df
# %%
