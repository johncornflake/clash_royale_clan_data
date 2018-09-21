#!usr/bin/python

import requests
import urllib.parse
import pandas as pd
import pymysql as mysql
from datetime import datetime
import sys
import os
from pprint import pprint

start_time = datetime.now()

urlparse = urllib.parse.quote_plus

rootDir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, rootDir)
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
    try:
        location = row['location']
    except:
        location = None
    if row.get('isCountry') == 'False':
        isCountry = 0
    else:
        isCountry = 1
    r.append(row.get('tag'))
    r.append(row.get('name'))
    r.append(row.get('badgeId'))
    r.append(row.get('type'))
    r.append(row.get('clanScore'))
    r.append(row.get('requiredTrophies'))
    r.append(row.get('donationsPerWeek'))
    r.append(row.get('clanChestLevel'))
    r.append(row.get('clanChestMaxLevel'))
    r.append(row.get('members'))
    if location == None:
        r.extend([None, None, None, None])
    else:
        r.append(location.get('id'))
        r.append(location.get('name'))
        r.append(isCountry)
        r.append(location.get('name'))
    r.append(row.get('description'))
    r.append(row.get('clanChestStatus'))
    r.append(row.get('clanChestPoints'))
    clean_row = tuple(r)
    return clean_row

insert_query = '''
INSERT IGNORE INTO clan_information_log
VALUES (CURDATE(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW());
'''

# database connection and other variables from const_file
db_cxn = mysql.connect(host=const_file.db_host,
                        user=const_file.db_username,
                        password=const_file.db_password,
                        db=const_file.db_schema,
                        charset="utf8mb4")

clan_tag = urlparse(const_file.clan_tag)
headers = const_file.headers
base_url = const_file.base_url

# get clan data
clan_url = base_url + 'clans/' + clan_tag
response = requests.get(clan_url, headers=headers)
clan_data = response.json()

if response.status_code != 200:
    print('%s - %s: %s' % (response.status_code, clan_data['reason'], clan_data['message']))
    exit()

# NEED TO CONVERT NON-STRING CHARACTERS, OR MAKE IT SO THE DATABASE CAN READ THOSE DANG EMOJIS
# add data to tuple and insert row, the disconnect from server
clan_row = [parseRow(clan_data)]
doInsert(clan_row)
db_cxn.close()

print(sys.argv[0] + ' ' + str(datetime.now() - start_time))
