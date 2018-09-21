#!usr/bin/python

from pprint import pprint
import requests
import urllib.parse
import pandas as pd

urlparse = urllib.parse.quote_plus
df = pd.DataFrame

def toCSV(data, name):
    f = df(data)
    file_name = name + '.csv'
    f.to_csv(file_name, index=False)

import const_file
token = const_file.cr_token
clan_tag = urlparse(const_file.clan_tag)
headers = {'Authorization': "Bearer:" + token}
base_url = 'https://api.clashroyale.com/v1/'

s = requests.Session()

# setup clan data
clan_url = base_url + 'clans/' + clan_tag
r = requests.get(clan_url, headers=headers)
clan_data = r.json()
members = clan_data['memberList']
w = requests.get(clan_url + '/warlog', headers=headers)
warlog_data = w.json()
toCSV(warlog_data['items'], 'warlog')

exit()
players_url = base_url + "players/"
for player in members:
    tag = urlparse(player['tag'])
    p_response = requests.get(players_url + tag, headers=headers)
    player_data = p_response.json()
    pprint(player_data)
    exit()
    card_data = player_data['cards']

    # battle data
    b_response = requests.get(players_url + tag + '/battlelog', headers=headers)
    battle_data = b_response.json()
