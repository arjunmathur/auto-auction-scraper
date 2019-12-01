import json
import re
import csv
import requests
import os.path
import datetime
from bs4 import BeautifulSoup, SoupStrainer

from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm


BAT_RESULTS_URL = 'https://bringatrailer.com/porsche/993/'

RAW_AUCTIONS_FILENAME = 'raw_auctions.json'
RAW_AUCTIONS_DETAILED_FILENAME = 'raw_auctions_detailed.json'
ENRICHED_AUCTIONS_FILENAME = 'auctions.csv'

def main():
    print('1. Pulling Raw Auctions')
    raw_auctions = _load_snapshot(RAW_AUCTIONS_FILENAME) or pull_raw_auctions()
    
    print('2. Pulling Details for Raw Auctions')
    raw_auctions_detailed = _load_snapshot(RAW_AUCTIONS_DETAILED_FILENAME) or pull_raw_auction_details(raw_auctions)
    
    print('3. Enriching Auctions')
    auctions = enrich_auctions(raw_auctions_detailed)
    
    print('4. Dumping to CSV')
    keys = ['title', 'url', 'sold', 'details', 'model', 'transmission', 'mileage', 'date', 'year', 'amount']
    with open(ENRICHED_AUCTIONS_FILENAME, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(auctions)


def pull_raw_auctions():
    # pull data
    response = requests.get(BAT_RESULTS_URL)
    page = BeautifulSoup(response.content, 'lxml')
    data = page.find(class_='chart')['data-stats']
    raw_auctions = json.loads(data)
    
    # flatten auctions
    sold = raw_auctions['s']
    unsold = raw_auctions['u']
    for c in sold:
        c['sold'] = True
    for c in unsold:
        c['sold'] = False
    raw_auctions = sold + unsold

    with open(RAW_AUCTIONS_FILENAME, 'w') as f:
        json.dump(raw_auctions, f)
    return raw_auctions


def pull_raw_auction_details(raw_auctions):
    with ThreadPoolExecutor(max_workers=5) as pool:
        result = []
        futures = [pool.submit(update_auction_details, auction) for auction in raw_auctions]
        for f in tqdm(as_completed(futures)):
            result.append(f.result())
        # auctions = map(update_auction_details, auctions)
        # auctions = pool.map(update_auction_details, auctions)
    with open(RAW_AUCTIONS_DETAILED_FILENAME, 'w') as f:
        json.dump(result, f)


def enrich_auctions(auctions):
    for auction in auctions:
        auction['model'] = _get_model(auction['title'])
        auction['transmission'] = _get_transmission(auction['title'], auction['details'], auction['model'])
        auction['mileage'] = _get_mileage(auction['title'], auction['details'])
        auction['date'] = datetime.date.fromtimestamp(auction['timestamp'])
        auction['year'] = auction['date'].year
        del auction['timestamp']
        del auction['titlesub']
        del auction['timestampms']
        del auction['image']
    return auctions



#########################################################################################################


def pull_auctions_details(auctions):
    with ThreadPoolExecutor(max_workers=5) as pool:
        result = []
        futures = [pool.submit(update_auction_details, auction) for auction in auctions]
        for f in tqdm(as_completed(futures)):
            result.append(f.result())
        # auctions = map(update_auction_details, auctions)
        # auctions = pool.map(update_auction_details, auctions)
    return result
    

def update_auction_details(auction):
    response = requests.get(auction['url'])
    page = BeautifulSoup(response.content, 'lxml')
    auction_info = page.find_all(class_='listing-essentials-item')
    auction['details'] = [a.text for a in auction_info]
    return auction




#########################################################################################################

def _get_model(title):
    title = title.lower()
    if 'cabriolet' in title:
        return 'cabriolet'
    if 'targa' in title:
        return 'targa'
    if '2s' in title or 'carrera s' in title:
        return 'c2s'
    if '4s' in title:
        return 'c4s'
    if 'turbo' in title:
        return 'turbo'
    if 'carrera 4' in title:
        return 'c4'
    return 'c2'



def _get_transmission(title, details, model):
    info = [title.lower()] + [d.lower() for d in details]
    if any(['6-speed' in d or '6 speed' in d or 'six-speed' in d for d in info]):
        return 'manual'
    if any('tiptronic' in d or 'automatic' in d for d in info):
        return 'tiptronic'
    
    # best guess
    if model in ('c2s', 'c4s', 'turbo'):
        return 'manual'
    return 'tiptronic'


def _get_mileage(title, details):
    regex = r"([\dxk,]+)(?: \w+)?[ -]miles"
    for info in ([title] + details):
        match = re.search(regex, info, re.IGNORECASE)
        if match:
            result = match.group(1)
            return _parse_mileage_number(result)
    return ''
    

def _parse_mileage_number(num):
    num = num.lower()
    num = num.replace('k', '000')
    num = num.replace(',', '')
    num = num.replace('x', '0')
    return num


def _load_snapshot(filename):
    if not os.path.isfile(filename):
        return None
    with open(filename) as f:
        return json.load(f)


if __name__ == '__main__':
    main()

# no reserve
# clean carfax
# roller