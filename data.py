#!python3

import pandas as pd
from scipy import where
from sodapy import Socrata
import csv, sqlite3
import datetime
from datetime import timedelta, date
import requests
import os

def init_db():
    db = sqlite3.connect('data.db')

    cursor = db.cursor()
    tables = []

    for row in cursor.execute("SELECT name FROM sqlite_master;"):
        tables.append(row)
    
    if ('vax',) not in tables:
        sql_file = open("sql/vax.sql")
        sql_script = sql_file.read()
        cursor.executescript(sql_script)
        print('vax table initialized')
    
    if ('cases',) not in tables:
        sql_file = open("sql/cases.sql")
        sql_script = sql_file.read()
        cursor.executescript(sql_script)
        print('cases table initialized')
    
    if ('real_estate',) not in tables:
        sql_file = open("sql/real_estate.sql")
        sql_script = sql_file.read()
        cursor.executescript(sql_script)
        print('real estate table initialized')
    
    db.commit()
    db.close()

def extract_vax():
    url = "data.cdc.gov"
    endpoint = "8xkx-amqh"
        
    if is_cached('vax'):
        return

    yesterday = str(date.today() - timedelta(days=1))
    client = Socrata(url, None)
    offset = 0
    chunk = 500
    results = []
    count = client.get(endpoint, select="COUNT(*)", where = "date = '" + yesterday + "'")

    while True:

        results.extend((
            client.get(
                endpoint,
                select = "date, fips, recip_county, recip_state, series_complete_pop_pct",
                where = "date = '" + yesterday + "'",
                limit=chunk, 
                offset=offset
                )))

        offset += chunk
        print(offset, ' rows extracted.')
        if (offset > int(count[0]['COUNT'])):
            break
    
    df = pd.DataFrame.from_records(results)
    df.to_csv(r'./vax.csv')

def store_vax():

    if is_cached('vax'):
        print("vax cached inside store")
        return

    db = sqlite3.connect('data.db')
    cursor = db.cursor()

    with open('vax.csv', 'r') as csv_file:
        data = csv.DictReader(csv_file)
        to_sql = [ (i['date'],i['fips'],i['recip_county'],i['recip_state'], i['series_complete_pop_pct']) for i in data] 
        cursor.executemany(
            "INSERT INTO vax (date, fips, recip_county, recip_state, series_complete_pop_pct)"
            " VALUES (?, ?, ?, ?, ?)"
            , to_sql
        )
    print('Data stored in vax table.')
    
    db.commit()
    db.close()
    
    os.remove('vax.csv')

def extract_cases():
    url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-recent.csv'

    if is_cached('cases'):
        return
    
    with requests.Session() as s:
        data = s.get(url)
        decoded_content = data.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        arr = list(cr)

    header = arr.pop(0)
    format = '%Y-%m-%d'
    latest = datetime.datetime.strptime(arr[len(arr)-1][0], format)

    clean = []

    for elem in arr:
        date = datetime.datetime.strptime(elem[0], format)
        if date == latest:
            clean.append(elem)
        
    with open('cases.csv', 'w') as f: 
        write = csv.writer(f) 
        write.writerow(header) 
        write.writerows(clean)

def store_cases():

    if is_cached('cases'):
        return

    db = sqlite3.connect('data.db')
    cursor = db.cursor()

    with open('cases.csv', 'r') as csv_file:
        data = csv.DictReader(csv_file)
        to_sql = [ (i['date'],i['county'],i['state'],i['fips'], i['cases'], i['deaths']) for i in data] 
        cursor.executemany(
            "INSERT INTO cases (date, county, state, fips, cases, deaths)"
            " VALUES (?, ?, ?, ?, ?, ?)"
            , to_sql
        )
    print('Data stored in cases table.')
    db.commit()
    db.close()

    os.remove('cases.csv')


def store_real_estate():
    # ensure data is in the agreed format first 
    # add speed bump here

    db = sqlite3.connect('data.db')
    cursor = db.cursor()

    with open('static_data/real_estate.csv', 'r') as csv_file:
        data = csv.DictReader(csv_file)
        to_sql = [( i['date'], i['fips'], i['state_comp'], i['state_short'], i['median_listing_price'], i['median_change_pct'], i['median_days_on_market'], i['average_listing_price'], i['avg_price_change']) for i in data] 
        cursor.executemany(
            "INSERT INTO real_estate (date, fips, state_comp, state_short, median_listing_price, median_change_pct,median_days_on_market,average_listing_price,avg_price_change)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            , to_sql
        )
    print('Data stored in real estate table.')
    db.commit()
    db.close()
     


def is_cached(table):
    cached = True
    db = sqlite3.connect('data.db')
    db.row_factory = sqlite3.Row
    cursor = db.cursor()
    cursor.execute("SELECT date FROM " + table + " LIMIT 1;")
    result = [dict(row) for row in cursor.fetchall()]

    if not result:
        return (not cached)
    else:
        date = result[0]['date']
        if table == 'vax':
            most_recent = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")
        elif table == 'cases':
            most_recent = datetime.datetime.strptime(date, "%Y-%m-%d")
        curr_time = datetime.datetime.now()

        if (curr_time.date() - most_recent.date()).days <= 1:
            return cached
            
    return (not cached)
