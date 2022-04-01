DROP TABLE IF EXISTS infection;

CREATE TABLE infection (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	date TIMESTAMP,
	county TEXT, 
	state TEXT,
	fips TEXT, 
	cases INTEGER,
    deaths INTEGER
);