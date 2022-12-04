import json
import requests

def json_scraper(url, file_name, bucket):
	print('start running')
	response = requests.request("GET", url)
	json_data = response.json()
	with open(file_name, 'w', encoding='utf-8') as json_file:
		json.dump(json_data, json_file, ensure_ascii=False, indent=4)
	print('end running')

# json_scraper('https://www.predictit.org/api/marketdata/all', 'predict_market.json', 'data-mbfr')