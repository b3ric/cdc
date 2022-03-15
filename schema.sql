DROP TABLE IF EXISTS cdc;

CREATE TABLE cdc (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	date TIMESTAMP,
	fips TEXT, 
	recip_county TEXT,
	recip_state TEXT, 
	series_complete_pop_pct INTEGER
);