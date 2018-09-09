#!usr/bin/Python3

import pymysql as mysql
import sys
from datetime import datetime

start_time = datetime.now()

import const_file

db_cxn = mysql.connect(host=const_file.db_host,
                        user=const_file.db_username,
                        password=const_file.db_password,
                        db=const_file.db_schema)

create_tables = [{'table_name': 'clan_members',
                'sql': '''
                CREATE TABLE IF NOT EXISTS clan_members (
                    tag varchar(64),
                    player_name varchar(155),
                    player_level int,
                    player_trophies int,
                    arena_id int,
                    arena_name varchar(64),
                    role varchar(64),
                    clan_rank int,
                    previous_clan_rank int,
                    clan_chest_points int,
                    donations int,
                    donations_received int,
                    refreshed_at timestamp,
                    PRIMARY KEY (tag)
                ) DEFAULT CHARSET=utf8;'''},
                {'table_name': 'player_details',
                'sql': '''
                CREATE TABLE IF NOT EXISTS player_details (
                	tag varchar(64),
                    as_of_date DATE,
                	name varchar(155),
                	exp_level int,
                	trophies int,
                	arena_id int,
                	arena_name varchar(64),
                	bestTrophies int,
                	wins int,
                	losses int,
                	battle_count int,
                	three_crown_wins int,
                	challenge_cards_won int,
                	challenge_max_wins int,
                    tournament_cards_won int,
                    tournament_battle_count int,
                    role varchar(64),
                    donations int,
                    donations_received int,
                    total_donations int,
                    war_day_wins int,
                    clan_cards_collected int,
                    current_league_trophies int,
                    current_best_trophies int,
                    previous_league_id varchar(64),
                    previous_league_trophies int,
                    previous_best_trophies int,
                    best_season_id varchar(64),
                    best_season_trophies int,
                    best_season_best_trophies int,
                    current_favourite_card_id int,
                    current_favourite_card_max_level int,
                    UNIQUE KEY player_details_uk(tag, battle_count),
                    INDEX tag_idx (tag)
                ) DEFAULT CHARSET=utf8;'''},
                {'table_name': 'cards',
                'sql': '''
                CREATE TABLE IF NOT EXISTS cards (
                    id int,
                    name varchar(155),
                    max_level int,
                    icon_url varchar(1024),
                    refreshed_at timestamp,
                    PRIMARY KEY (id)
                    ) DEFAULT CHARSET=utf8;'''}
                ]

def createTable(table_data):
    table_name = table_data['table_name']
    query = table_data['sql']
    db_cxn.begin()
    cursor = db_cxn.cursor()
    try:
        cursor.execute(query)
        db_cxn.commit()
    except:
        db_cxn.rollback()
        print('failed to create %s \n' % (table_name))
        raise
    finally:
        cursor.close()

for table in create_tables:
    createTable(table)

db_cxn.close()

print(sys.argv[0] + ' completed successfully.' + str(datetime.now() - start_time))
