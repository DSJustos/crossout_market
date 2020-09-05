# -*- coding: utf-8 -*-

import requests
import numpy as np
import pandas as pd
import time
import configparser
from tqdm import tqdm
tqdm.pandas()

import time_series_extraction as tseries
import instant_deal as i_deal

def apply_filters(df, factions=[''], categories=[''], rarities=[''], popularity_threshold=0, available_money=99999, low_price_threshold=0):
    filter_factions = np.ones(len(df_items), dtype=bool) if factions==[''] else df.faction.isin(factions)
    filter_categories = np.ones(len(df_items), dtype=bool) if categories==[''] else df.categoryName.isin(categories)
    filter_rarities = np.ones(len(df_items), dtype=bool) if rarities==[''] else df.rarityName.isin(rarities)
    
    threshold_popularity = df.buyOrders > popularity_threshold
    threshold_cost = (df.formatBuyPrice < available_money) & (df.formatBuyPrice > low_price_threshold) # | (df.formatCraftingBuySum < available_money)
    
    df_return = df.copy()
    df_return = df_return[(filter_factions) & (filter_categories) & (filter_rarities) & (threshold_popularity) & (threshold_cost)]
    return df_return

config = configparser.ConfigParser()
config.read('config.ini')

###############################################################################
print("\nCalling API...")
resp_items = requests.get('https://crossoutdb.com/api/v1/items')

if resp_items.status_code != 200:
    # This means something went wrong.
    print("GET request for items failed")

df_raw = pd.DataFrame.from_records(resp_items.json())
df_items = df_raw[["id","name","sellOffers","formatSellPrice","buyOrders","formatBuyPrice","rarityName","categoryName","typeName","faction",
                     "formatCraftingSellSum", # comprar os components imediatamente
                     "formatCraftingBuySum", # comprar os components por order
                     "formatMargin", # margem, assumindo compra e venda por orders ao preço atual tendo em conta a taxa (flipping)
                     "craftingMargin", # margem, assumindo compra de components em buy order e venda do resultado em sell offer, taxas incluidas (crafting)
                     ]]
df_items[["sellOffers","formatSellPrice","buyOrders",
          "formatBuyPrice","formatCraftingSellSum",
          "formatCraftingBuySum","formatMargin","craftingMargin"]] = df_items[["sellOffers","formatSellPrice","buyOrders",
                                                                               "formatBuyPrice","formatCraftingSellSum","formatCraftingBuySum",
                                                                               "formatMargin","craftingMargin"]].astype(float)
df_items.loc[:,"craftingMargin"] = df_items["craftingMargin"]/100
print("Done!")

###############################################################################
print("\nChecking instant deals...")
i_deal.get_instant_deals(df_items[['id','name','rarityName','categoryName','formatSellPrice','formatBuyPrice']])
print("Done!")

###############################################################################
print("\nApplying filters and thresholds...")

factions = config['FILTERS']['FACTIONS'].split(",")
categories = config['FILTERS']['CATEGORIES'].split(",")
rarities = config['FILTERS']['RARITIES'].split(",")

popularity_threshold = int(config['BASELINE_THRESHOLDS']['BUY_ORDERS_LOW_THRESHOLD'])
available_money = float(config['BASELINE_THRESHOLDS']['AVAILABLE_MONEY'])
low_price_threshold = float(config['BASELINE_THRESHOLDS']['BUY_PRICE_LOW_THRESHOLD'])

df_filtered = apply_filters(df=df_items, factions=factions,
                            categories=categories, rarities=rarities,
                            popularity_threshold=popularity_threshold,
                            available_money=available_money,
                            low_price_threshold=low_price_threshold)
print("Done!")


start = time.time()
###############################################################################
print("\nRunning parallelized time series extraction...")
current_time = int(time.time())
t_minus1week = int(current_time - 604800)
    
df_tseries = tseries.parallelized_get_tseries_params(item_id_list=df_filtered['id'].tolist(), t0_time=current_time, t_minus1week_time=t_minus1week)
df_final = pd.concat([df_filtered.reset_index(drop=True), df_tseries], axis=1, sort=False)
df_final['off_dem_abs'] = df_final.sellOffers - df_final.buyOrders
df_mesmo_final = df_final[['name', 'formatBuyPrice', 'formatSellPrice', 'formatMargin', 'buy_orders_std', 'demoff_diff', 'buyprice_20percentile', 'sellprice_80percentile','off_dem_abs']]
df_mesmo_final.loc[:,'immediate_buy_margin'] = df_mesmo_final['sellprice_80percentile']*0.9 - df_mesmo_final['formatBuyPrice']
df_mesmo_final.loc[:, 'week_buy_margin'] = df_mesmo_final['sellprice_80percentile']*0.9 - df_mesmo_final['buyprice_20percentile']
df_mesmo_final.loc[:, 'ROI_immediate'] = df_mesmo_final['immediate_buy_margin']/df_mesmo_final['formatBuyPrice']
df_mesmo_final.loc[:, 'ROI_weekly'] = df_mesmo_final['week_buy_margin']/df_mesmo_final['buyprice_20percentile']
print("Done!")
end = time.time()
print(f'Time elapsed: {round(end - start, 4)}s')


###############################################################################
# DEACTIVATED this temporarily to test the paralelized implementation!
if True or len(df_filtered)==0:
    print("No rows left after filtering. Exiting...")
else:
    print("\nComputing time series...")
    current_time = int(time.time())
    t_minus1week = int(current_time - 604800)
    
    df_filtered[['sell_diff_to_linear', 'sell_alpha',
                 'demoff_diff_to_linear', 'demoff_alpha',
                 'buyOrders_std']] = df_filtered.progress_apply(lambda x: tseries.get_tseries_params(x['id'],
                                                                                                     current_time,
                                                                                                     t_minus1week), axis=1)
    print("Done!")