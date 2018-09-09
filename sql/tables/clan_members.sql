-- clan_members table

CREATE TABLE clan_members (
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
	) DEFAULT CHARSET=utf8
;
