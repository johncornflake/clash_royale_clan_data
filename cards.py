#!usr/bin/Python3

import requests
import urllib.parse
import pandas as pd
import pymysql as mysql
from datetime import datetime
import sys
import warnings
from pprint import pprint

start_time = datetime.now()

urlparse = urllib.parse.quote_plus

import const_file

def doInsert(insert_data):
    db_cxn.begin()
    cursor = db_cxn.cursor()
    try:
        cursor.executemany(insert_query, insert_data)
        db_cxn.commit()
    except:
        db_cxn.rollback()
        raise
    finally:
        cursor.close()

def parseRow(row):
    r = []
    r.append(row.get('id'))
    r.append(row.get('name'))
    r.append(row.get('maxLevel'))
    r.append(row['iconUrls'].get('medium'))
    clean_row = tuple(r)
    return clean_row

insert_query = '''
REPLACE INTO cards
VALUES (%s, %s, %s, %s, now());
'''

truncate_query = 'TRUNCATE cards;'

# database connection and other variables from const_file
db_cxn = mysql.connect(host=const_file.db_host,
                        user=const_file.db_username,
                        password=const_file.db_password,
                        db=const_file.db_schema)

headers = const_file.headers
base_url = const_file.base_url

# get players
card_url = base_url + 'cards'
response = requests.get(card_url, headers=headers)
card_data = response.json()['items']
if response.status_code != 200:
    print('%s - %s: %s' % (response.status_code, clan_data['reason'], clan_data['message']))
    exit()

# loop through all members, putting them in a tuple and appending to a list
card_list = []
for card in card_data:
    card_list.append(parseRow(card))

# truncate table before populating new cards
db_cxn.begin()
cursor = db_cxn.cursor()
try:
    cursor.execute(truncate_query)
    db_cxn.commit()
except:
    db_cxn.rollback()
    raise
finally:
    cursor.close()

# insert cards into table and disconnect from server
doInsert(card_list)
db_cxn.close()

print(sys.argv[0] + ' ' + str(datetime.now() - start_time))
