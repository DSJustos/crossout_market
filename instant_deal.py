# -*- coding: utf-8 -*-
"""
Created on Thu Aug  6 19:43:33 2020

@author: Justos
"""

import pandas as pd

def get_instant_deals(df):
    local_df = df.copy()
    tier_sales = compute_tier_instant_sales(local_df)
    local_df = local_df.merge(tier_sales, left_on='rarityName', right_on='rarityName')
    local_df = local_df[(local_df.categoryName != 'Customization') & (local_df.categoryName != 'Dyes') & (local_df.categoryName != 'Resources')]
    
    local_df['dismantle_buy_margin'] = local_df['dismantle_buy'] - local_df['formatSellPrice']
    local_df['dismantle_sell_margin'] = local_df['dismantle_sell'] - local_df['formatSellPrice']
    
    
    if len(local_df[local_df['dismantle_buy_margin']>0]):
        print("#################################")
        print("INSTANT DISMANTLE DEALS!")
        print("#################################")
        print(local_df[local_df['dismantle_buy_margin']>0][['name','dismantle_buy_margin']].sort_values(by=['dismantle_buy_margin'], ascending=False))
    else:
        print("No instant dismantle deals")
        
    
    if len(local_df[local_df['dismantle_sell_margin']>0]):
        print("#################################")
        print("BY SELL ORDER DISMANTLE DEALS!")
        print("#################################")
        print(local_df[local_df['dismantle_sell_margin']>0][['name','dismantle_sell_margin']].sort_values(by=['dismantle_sell_margin'], ascending=False))
    else:
        print("No sell order dismantle deals")


def compute_tier_instant_sales(df):
    df_dismantles = pd.DataFrame(columns=['rarityName', 'dismantle_buy', 'dismantle_sell'])
    
    for rarity in ['Common','Rare','Special','Epic','Legendary','Relic']:
        discounted_buy, discounted_sell = compute_rarity_dismantle(df, rarity)
        df_dismantles = df_dismantles.append({'rarityName': rarity, 'dismantle_buy': discounted_buy, 'dismantle_sell': discounted_sell}, ignore_index=True)
    
    return df_dismantles


def compute_rarity_dismantle(df, rarity):
    
    switcher = {
        'Common': [0.01, 0, 0, 0, 0, 0],
        'Rare': [1.5, 0.5, 0, 0, 0, 0],
        'Special': [0.4, 1, 0.6, 0.3, 0, 0],
        'Epic': [0.8, 1.5, 1.7, 0.8, 0, 0],
        'Legendary': [0.5, 2.5, 0, 0, 2.5, 2.5],
        'Relic': [0, 5, 0, 0, 5, 3.5]
    }
    ratios = switcher[rarity]
    
    
    buy_price = df[df.name=='Scrap Metal x100'][['formatBuyPrice']].values[0][0]*ratios[0] + \
                df[df.name=='Copper x100'][['formatBuyPrice']].values[0][0]*ratios[1] + \
                df[df.name=='Wires x100'][['formatBuyPrice']].values[0][0]*ratios[2] + \
                df[df.name=='Plastic x100'][['formatBuyPrice']].values[0][0]*ratios[3] + \
                df[df.name=='Electronics x100'][['formatBuyPrice']].values[0][0]*ratios[4] + \
                df[df.name=='Batteries x100'][['formatBuyPrice']].values[0][0]*ratios[5]
               
    sell_price = df[df.name=='Scrap Metal x100'][['formatSellPrice']].values[0][0]*ratios[0] + \
                 df[df.name=='Copper x100'][['formatSellPrice']].values[0][0]*ratios[1] + \
                 df[df.name=='Wires x100'][['formatSellPrice']].values[0][0]*ratios[2] + \
                 df[df.name=='Plastic x100'][['formatSellPrice']].values[0][0]*ratios[3] + \
                 df[df.name=='Electronics x100'][['formatSellPrice']].values[0][0]*ratios[4] + \
                 df[df.name=='Batteries x100'][['formatSellPrice']].values[0][0]*ratios[5]
    
    # accounting for 10% fee
    return buy_price*0.9, sell_price*0.9
                 
                 