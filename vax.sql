DROP TABLE IF EXISTS vax;

CREATE TABLE vax (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	date TIMESTAMP,
	fips TEXT, 
	recip_county TEXT,
	recip_state TEXT, 
	series_complete_pop_pct INTEGER
);