DROP TABLE IF EXISTS real_estate;

CREATE TABLE real_estate (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	date TIMESTAMP,
	fips TEXT, 
	state_comp TEXT,
	state_short TEXT, 
	median_listing_price INTEGER,
    median_change_pct FLOAT,
    median_days_on_market INTEGER, 
    average_listing_price INTEGER, 
    avg_price_change FLOAT
);