from datetime import datetime,timedelta
import pandas as pd
import csv
import requests
import json
import asyncio
from dexguru_sdk import DexGuru

APIKEY = 'euyd-T5oU_kSEosC4OnED_lX8nxr5aWYNdGhWelx57E'
sdk = DexGuru(api_key=APIKEY)
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

def download_data(exclude):
	df = pd.read_csv('~/Documents/nft_yield.csv')
	df = df.query("Name not in @exclude")
	df = df.dropna()
	df['Date'] = pd.to_datetime("now").strftime("%Y-%m-%d")
	cols = df.columns.tolist()
	cols = cols[-1:] + cols[:-1]
	df = df[cols]
	return df

async def main(addresses):
	result = []
	for address in addresses:
		try:
			response = await sdk.get_token_finance('1',address)
			result.append(response.dict()['price_usd'])
		except:
			result.append(-1)
	return result

def pull_prices_floors(df):
	addresses = df['Token contract'].tolist()
	addresses.append(WETH)
	if __name__ == '__main__':
		prices = asyncio.run(main(addresses))
	ETH_price = prices[len(prices)-1]
	df['Token price'] = prices[0:len(prices)-1]
	floors = []
	dates_created = []
	for slug in df['Slug'].tolist():
		opensea_url = "https://api.opensea.io/collection/"+slug+"?format=json"
		response = requests.request("GET", opensea_url)
		response_json = response.json()
		floor = response_json['collection']['stats']['floor_price']
		date_created = response_json['collection']['primary_asset_contracts'][0]['created_date']
		datetime_created = datetime.strptime(date_created, '%Y-%m-%dT%H:%M:%S.%f')
		floors.append(floor)
		dates_created.append(datetime_created)
	df['Floor'] = floors
	df['floor + gas'] = df['Floor']+0.015
	df['floor + gas (usd)'] = df['floor + gas']*ETH_price
	df['datetimes_created'] = dates_created
	return df

def override_price(df,token,token_price):
	df.loc[df['Token name'] == token, 'Token price'] = token_price
	return df

def organize(df):
	df = df[df['Token price']!= -1 ]
	df = df[df['Token price']!= 0 ]
	df['rewards'] = df['Token price'] * df['Daily reward']
	df['APY'] = df['rewards'] * 365 / df['floor + gas (usd)']
	df['Days to breakeven'] = df['floor + gas (usd)']/df['rewards']
	df['Days since mint'] = (datetime.today() - df['datetimes_created']).dt.days
	df['APY'] = (df['APY']*100).astype(int).astype(str) + '%'
	df['Days to breakeven'] = df['Days to breakeven'].astype(int)
	df['floor + gas (usd)'] = df['floor + gas (usd)'].round(1)
	df.drop(['Slug','Token contract','rewards','datetimes_created','floor + gas','floor + gas (usd)'], axis=1, inplace=True)
	df = df.sort_values(by=['Days to breakeven'])
	return df

def earlier_data_comparison(df,df_all,period):
	pre_3d = (datetime.strptime(df['Date'][0], '%Y-%m-%d') - timedelta(days=period)).strftime('%Y-%m-%d')
	df_pre3d = df_all[['Name','Floor','Token price']][df_all['Date']==pre_3d]
	df_pre3d.rename(columns={'Floor': 'Floor 3d ago','Token price': 'Token price 3d ago'}, inplace=True)
	df_merged = pd.merge(df,df_pre3d,on='Name')
	df_merged['Floor change 3d'] = (df_merged['Floor']-df_merged['Floor 3d ago'])/df_merged['Floor 3d ago']
	df_merged['Floor change 3d'] = (df_merged['Floor change 3d']*100).astype(int).astype(str) + '%'
	df_merged['Token $ change 3d'] = (df_merged['Token price']-df_merged['Token price 3d ago'])/df_merged['Token price 3d ago']
	df_merged['Token $ change 3d'] = (df_merged['Token $ change 3d']*100).astype(int).astype(str) + '%'
	df_merged.drop(['Floor 3d ago','Token price 3d ago'], axis=1, inplace=True)
	return df_merged


def add_to_database(df):


exclude = ["CyberKongz Genesis","Kaiju Genesis"]
df = download_data(exclude)
df = pull_prices_floors(df)
df
df = override_price(df,"$BLOOD",1.41)
df = override_price(df,"$BRAINZ",0.2977542)
df = override_price(df,"$SMOKE",0.4313098)
df = organize(df)


df_merged = earlier_data_comparison(df,df_all,3)


df_all = df_all.append(df)
df_all.to_csv('nft_yieldoor_all.csv')



#addresses = ['0x2604e9f68259e609e8744fb67cc410d50fc9aa0f', '0x5cd2fac9702d68dde5a94b1af95962bcfb80fc7d', '0x4a87b3153c624530f7d79db58b2a9dc4befcd433']

#address = "0x5cd2fac9702d68dde5a94b1af95962bcfb80fc7d"
#async def main():
#	response = await sdk.get_token_finance('1',address)
#	return response

#if __name__ == '__main__':
#	asyncio.run(main())
