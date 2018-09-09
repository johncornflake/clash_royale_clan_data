#!usr/bin/Python3

import requests
import urllib.parse
import pandas as pd
import pymysql as mysql
from datetime import datetime
import sys

start_time = datetime.now()

urlparse = urllib.parse.quote_plus

import const_file

def doInsert(insert_data):
    db_cxn.begin()
    cursor = db_cxn.cursor()
    insert_length = len(insert_data)
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
    r.append(row.get('tag'))
    r.append(row.get('name'))
    r.append(row.get('expLevel'))
    r.append(row.get('trophies'))
    r.append(row['arena'].get('id'))
    r.append(row['arena'].get('name'))
    r.append(row.get('role'))
    r.append(row.get('clanRank'))
    r.append(row.get('previousClanRank'))
    r.append(row.get('clanChestPoints'))
    r.append(row.get('donations'))
    r.append(row.get('donationsReceived'))
    clean_row = tuple(r)
    return clean_row

insert_query = '''
REPLACE INTO clan_members
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
'''

db_cxn = mysql.connect(host=const_file.db_host,
                        user=const_file.db_username,
                        password=const_file.db_password,
                        db=const_file.db_schema)

clan_tag = urlparse(const_file.clan_tag)
headers = const_file.headers
base_url = const_file.base_url

clan_url = base_url + 'clans/' + clan_tag

# get player IDs
response = requests.get(clan_url, headers=headers)
clan_data = response.json()
members = clan_data['memberList']

member_list = []
for member in members:
    member_list.append(parseRow(member))

doInsert(member_list)

db_cxn.close()

print(sys.argv[0] + ' ' + str(datetime.now() - start_time))
