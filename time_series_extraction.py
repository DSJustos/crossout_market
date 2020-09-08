# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 19:43:33 2020

@author: luis-
"""

import requests
import pandas as pd
import seaborn as sns; sns.set()
import numpy as np
from sklearn.linear_model import LinearRegression
from tqdm import tqdm
import time

from concurrent.futures import ThreadPoolExecutor

def get_metrics(X, y, current_value, flag_plot=False):
    # linear regression fit
    reg = LinearRegression().fit(X.values.reshape(-1, 1), y.values.reshape(-1, 1))
    
    # Save model parameters
    lin_reg_params = [reg.coef_[0][0], reg.intercept_[0]]
    
    # Compute actual linear regression values
    linear_values = np.dot(X.values.reshape(-1, 1), np.array([lin_reg_params[0]])) + lin_reg_params[1]
    
    # Compute deviation from model metric
    model_diff = current_value - linear_values[-1]
    
    # Compute metric std
    metric_std = y.std()
    
    if flag_plot:
        frame = { 'var_y': y, 'index': X , 'linear_values': linear_values} 
        df = pd.DataFrame(frame) 
        # Plot tseries + linear model
        ax = sns.lineplot(x="index", y="var_y", data=df)
        ax = sns.lineplot(x="index", y="linear_values", data=df)
    
    return model_diff, lin_reg_params[0], metric_std
    

def get_tseries_params(item_id, t0_time, t_minus1week_time, flag_plot=False):

    call = f'https://crossoutdb.com/api/v1/market-all/{item_id}?startTimestamp={t_minus1week_time}&endTimestamp={t0_time}'
    resp_items = requests.get(call)
    
    if resp_items.status_code != 200:
        # This means something went wrong.
        print(f"GET request for item {item_id} failed")

    df_series = pd.DataFrame.from_records(resp_items.json(), columns=['id','sellprice','buyprice','selloffers','buyorders','datetime','UNIX_TIMESTAMP'])
    df_series = df_series.drop(columns=['UNIX_TIMESTAMP'])
    df_series.buyprice = df_series.buyprice/100
    df_series.sellprice = df_series.sellprice/100
    df_series['demand_over_offer'] = df_series.buyorders - df_series.selloffers
    df_series = df_series.reset_index()
    df_series.datetime = pd.to_datetime(df_series.datetime, infer_datetime_format=True)
    
    series_5m = df_series.set_index('datetime')
    series_1H = series_5m.resample('1H').mean().reset_index().reset_index()
    
    last_5m_sell = series_5m['sellprice'].iloc[-1]
    sell_diff, sell_alpha, sell_std = get_metrics(X=series_1H['index'], y=series_1H['sellprice'], current_value=last_5m_sell)
    
    last_5m_demoff = series_5m['demand_over_offer'].iloc[-1]
    demoff_diff, demoff_alpha, demoff_std = get_metrics(X=series_1H['index'], y=series_1H['demand_over_offer'], current_value=last_5m_demoff)
    
    """
    if flag_plot:
        # Plot tseries + linear model
        ax = sns.lineplot(x="index", y="sellprice", data=series_1H)
        ax = sns.lineplot(x="index", y="linear_reg_sell_price", data=series_1H)
    """
    
    return_series = pd.Series([sell_diff, sell_alpha, demoff_diff, demoff_alpha, series_1H.buyorders.std()])
    
    return return_series


###############################################################################
# EXPERIMENTAL
###############################################################################

def generate_url_list(item_id_list, t0_time, t_minus1week_time):
    return_list = [f'https://crossoutdb.com/api/v1/market-all/{item_id}?startTimestamp={t_minus1week_time}&endTimestamp={t0_time}' for item_id in item_id_list]
    return return_list

def get_url(pbar, url):
    resp = requests.get(url)
    if resp.status_code != 200:
            print(f"\nGET request failed for url: {url}")
    pbar.update(1)
    #time.sleep(0.5)    
    
    return resp

def process_paralelized_data(resp_item):
    df = pd.DataFrame.from_records(resp_item.json(), columns=['id','sellprice','buyprice','selloffers','buyorders','datetime','UNIX_TIMESTAMP'])
    
    if len(df)==0: return pd.DataFrame(np.zeros(5), columns=['sell_diff', 'sell_alpha', 'demoff_diff', 'demoff_alpha', 'buy_orders_std'])
    
    df = df.drop(columns=['UNIX_TIMESTAMP'])
    df.buyprice = df.buyprice/100
    df.sellprice = df.sellprice/100
    df['demand_over_offer'] = df.buyorders - df.selloffers
    df = df.reset_index()
    df.datetime = pd.to_datetime(df.datetime, infer_datetime_format=True)
    
    series_5m = df.set_index('datetime')
    series_1H = series_5m.resample('1H').mean().reset_index().reset_index()
    
    last_5m_sell = series_5m['sellprice'].iloc[-1]
    sell_diff, sell_alpha, sell_std = get_metrics(X=series_1H['index'], y=series_1H['sellprice'], current_value=last_5m_sell)
    
    last_5m_demoff = series_5m['demand_over_offer'].iloc[-1]
    demoff_diff, demoff_alpha, demoff_std = get_metrics(X=series_1H['index'], y=series_1H['demand_over_offer'], current_value=last_5m_demoff)
    
    return_df = pd.DataFrame([[sell_diff, sell_alpha, demoff_diff, demoff_alpha, series_5m.buyorders.std(), series_5m.sellprice.quantile(0.8), series_5m.buyprice.quantile(0.2), series_5m.buyprice.std()]],
                              columns=['sell_diff', 'sell_alpha', 'demoff_diff', 'demoff_alpha', 'buy_orders_std', 'sellprice_80percentile', 'buyprice_20percentile', 'buy_price_std'])
    
    return return_df


import functools

def parallelized_get_tseries_params(item_id_list, t0_time, t_minus1week_time):
    df_series = pd.DataFrame(columns=['sell_diff', 'sell_alpha', 'demoff_diff', 'demoff_alpha', 'buy_orders_std', 'sellprice_80percentile', 'buyprice_20percentile', 'buy_price_std'])
    
    list_of_urls = generate_url_list(item_id_list, t0_time, t_minus1week_time)
    
    with tqdm(total=len(item_id_list)) as pbar:
        with ThreadPoolExecutor(max_workers=8) as pool:
            ret = list(pool.map(functools.partial(get_url, pbar), list_of_urls))
    
    for r in ret:
        df_series = df_series.append(process_paralelized_data(r), ignore_index=True)
    
    return df_series