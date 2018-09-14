#!usr/bin/Python3

import requests
import urllib.parse
import pandas as pd
import pymysql as mysql
from datetime import datetime
import sys
from pprint import pprint

start_time = datetime.now()

urlparse = urllib.parse.quote_plus

import const_file

def formDate(string):
    y = string[:4]
    m = string[4:6]
    d = string[6:8]
    t = string[8:11] + ':' + string[11:13] + ':' + string[13:15]
    formatted_date = '%s-%s-%s%s' % (y, m, d, t)
    return formatted_date

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
        participants = row['participants']
    except:
        participants = None
    r.append(row.get('createdDate'))
    r.append(row.get('seasonId'))
    if participants == None:
        r.extend([None, None, None, None, None])
    else:
        r.append(participants[4])
        r.append(participants[3])
        r.append(participants[1])
        r.append(participants[2])
        r.append(participants[5])
    clean_row = tuple(r)
    return clean_row

insert_query = '''
REPLACE INTO war_log_players
VALUES (%s, %s, %s, %s, %s, %s, %s, now());
'''

# database connection and other variables from const_file
db_cxn = mysql.connect(host=const_file.db_host,
                        user=const_file.db_username,
                        password=const_file.db_password,
                        db=const_file.db_schema)

clan_tag = urlparse(const_file.clan_tag)
headers = const_file.headers
base_url = const_file.base_url

# need to add the AFTER thing
# get players
war_log_url = "%sclans/%s/warlog" % (base_url, clan_tag)
response = requests.get(war_log_url, headers=headers)
war_data = response.json()
if response.status_code != 200:
    print('%s - %s: %s' % (response.status_code, clan_data['reason'], clan_data['message']))
    exit()

# loop through wars, putting them in a tuple and appending to a list
player_list = []
# pprint(war_data['items'])
# exit()
for war in war_data['items']:
    created_at = formDate(war.get('createdDate'))
    season_id = war.get('seasonId')
    #participants = war['participants']
    #pprint(participants[0])
    #exit()
    try:
        participants = war['participants']
        for player in participants:
            r = [created_at, season_id]
            r.append(player['tag'])
            r.append(player.get('name'))
            r.append(player.get('cardsEarned'))
            r.append(player.get('collectionDayBattlesPlayed'))
            r.append(player.get('wins'))
            row = tuple(r)
            player_list.append(row)
    except:
        row = (created_at, season_id, None, None, None, None, None)
        player_list.append(row)

# insert members into table and disconnect from server
doInsert(player_list)
db_cxn.close()

print(sys.argv[0] + ' ' + str(datetime.now() - start_time))
