#!python3

import pandas as pd
from sodapy import Socrata
import csv, sqlite3
import datetime

url = "data.cdc.gov"
endpoint = "8xkx-amqh"

def init_db():
    db = sqlite3.connect('cdc.db')
    cursor = db.cursor()
    tables = []

    for row in cursor.execute("SELECT name FROM sqlite_master;"):
        tables.append(row)
    
    # table already in db
    if tables:
        return
        
    sql_file = open("schema.sql")
    sql_script = sql_file.read()
    cursor.executescript(sql_script)
    print('Database initialized')
    db.commit()
    db.close()

def extract_data():

    if cached():
        return
    
    client = Socrata(url, None)
    offset = 0
    chunk = 100
    results = []
    count = 1000
    #count = client.get(endpoint, select="COUNT(*)")

    while True:

        results.extend((
            client.get(
                endpoint, 
                limit=chunk, 
                offset=offset,
                select = "date, fips, recip_county, recip_state, series_complete_pop_pct"
                )))

        offset += chunk
        print(offset, ' rows extracted.')
        #if (offset > int(count[0]['COUNT'])):
        if (offset > count):
            break

    df = pd.DataFrame.from_records(results)
    df.to_csv(r'./data.csv')

def store():
    db = sqlite3.connect('cdc.db')
    cursor = db.cursor()

    with open('data.csv', 'r') as csv_file:
        data = csv.DictReader(csv_file)
        to_sql = [ (i['date'],i['fips'],i['recip_county'],i['recip_state'], i['series_complete_pop_pct']) for i in data] 
        cursor.executemany(
            "INSERT INTO cdc (date, fips, recip_county, recip_state, series_complete_pop_pct)"
            " VALUES (?, ?, ?, ?, ?)"
            , to_sql
        )
    print('Data stored in cdc table.')
    db.commit()
    db.close()

def cached():
    cached = True
    db = sqlite3.connect('cdc.db')
    db.row_factory = sqlite3.Row
    cursor = db.cursor()
    cursor.execute("SELECT date FROM cdc LIMIT 1;")
    result = [dict(row) for row in cursor.fetchall()]

    if not result:
        return (not cached)
    else:
        date = result[0]['date']
        most_recent = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
        curr_time = datetime.datetime.now()

        if (curr_time.date() - most_recent.date()).days <= 1:
            return cached
            
    return (not cached)
        
if __name__ == '__main__':
    init_db()
    extract_data()
    store()