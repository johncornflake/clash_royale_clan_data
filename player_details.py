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
        previousSeason = row['leagueStatistics']['previousSeason']
    except:
        previousSeason = None
    try:
        currentSeason = row['leagueStatistics']['currentSeason']
    except:
        currentSeason = None
    try:
        bestSeason = row['leagueStatistics']['bestSeason']
    except:
        bestSeason = None
    try:
        currentFavouriteCard = row['currentFavouriteCard']
    except:
        currentFavouriteCard = None
    r.append(row.get('tag'))
    r.append(row.get('name'))
    r.append(row.get('expLevel'))
    r.append(row.get('trophies'))
    r.append(row['arena'].get('id'))
    r.append(row['arena'].get('name'))
    r.append(row.get('bestTrophies'))
    r.append(row.get('wins'))
    r.append(row.get('losses'))
    r.append(row.get('battleCount'))
    r.append(row.get('threeCrownWins'))
    r.append(row.get('challengeCardsWon'))
    r.append(row.get('challengeMaxWins'))
    r.append(row.get('tournamentCardsWon'))
    r.append(row.get('tournamentBattleCount'))
    r.append(row.get('role'))
    r.append(row.get('donations'))
    r.append(row.get('donationsReceived'))
    r.append(row.get('totalDonations'))
    r.append(row.get('warDayWins'))
    r.append(row.get('clanCardsCollected'))
    if currentSeason == None:
        r.extend([None, None])
    else:
        r.append(currentSeason.get('trophies'))
        r.append(currentSeason.get('bestTrophies'))
    if previousSeason == None:
        r.extend([None, None, None])
    else:
        r.append(previousSeason.get('id'))
        r.append(previousSeason.get('trophies'))
        r.append(previousSeason.get('bestTrophies'))
    if bestSeason == None:
        r.extend([None, None, None])
    else:
        r.append(bestSeason.get('id'))
        r.append(bestSeason.get('trophies'))
        r.append(bestSeason.get('bestTrophies'))
    if currentFavouriteCard == None:
        r.extend([None, None])
    else:
        r.append(currentFavouriteCard.get('id'))
        r.append(currentFavouriteCard.get('maxLevel'))
    clean_row = tuple(r)
    return clean_row

insert_query = '''
INSERT IGNORE INTO player_details
VALUES  (%s,CURDATE(),%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s);
'''

# database connection and other variables from const_file
db_cxn = mysql.connect(host=const_file.db_host,
                        user=const_file.db_username,
                        password=const_file.db_password,
                        db=const_file.db_schema)

clan_tag = urlparse(const_file.clan_tag)
headers = const_file.headers
base_url = const_file.base_url

# get players
clan_url = base_url + 'clans/' + clan_tag
response = requests.get(clan_url, headers=headers)
clan_data = response.json()
if response.status_code != 200:
    print('%s - %s: %s' % (response.status_code, clan_data['reason'], clan_data['message']))
    exit()
members = clan_data['memberList']

# loop through all members, putting them in a tuple and appending to a list
player_details_data = []
for member in members:
    tag = urlparse(member['tag'])
    url = '%splayers/%s' % (base_url, tag)
    r = requests.get(url, headers=headers)
    player_data = r.json()
    player_details_data.append(parseRow(player_data))

# insert rows into table and disconnect from server
doInsert(player_details_data)
db_cxn.close()

print(sys.argv[0] + ' ' + str(datetime.now() - start_time))
