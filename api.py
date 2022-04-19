#!python3

#TODO: add requirements.txt

from imp import init_builtin
from re import L
from matplotlib import table
import pandas as pd
from scipy import where
from sodapy import Socrata
import csv, sqlite3
import datetime
from datetime import timedelta, date
import requests
import os
from sqlalchemy import Table

from flask import Flask, request, jsonify
from flask_cors import CORS

def init_db():
    db = sqlite3.connect('data.db')

    cursor = db.cursor()
    tables = ['vax','cases','real_estate']

    for table in tables:
        if not table_exists(table):
            sql_file = open("sql/%s.sql" % table)
            sql_script = sql_file.read()
            cursor.executescript(sql_script)
            print('%s table initialized.' % table)
        else:
            print('%s table already exists.' % table)
    
    db.commit()
    db.close()

def is_cached(table):

    if not table_exists(table):
        raise TableNotInitialized('%s table has not been initialized!' % table) 

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


def extract_vax():
    url = "data.cdc.gov"
    endpoint = "8xkx-amqh"
    
    try:
        if is_cached('vax'):
            print("vax data already cached!")
            return
    except TableNotInitialized as e:
        print(e)
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

def extract_cases():
    url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-recent.csv'

    try: 
        if is_cached('cases'):
            return
    except TableNotInitialized as e: 
        print(e)
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

def store_vax():

    try:
        if is_cached('vax'):
            print("vax data already cached!")
            return
    except TableNotInitialized as e:
        print(e)
        return

    db = sqlite3.connect('data.db')
    cursor = db.cursor()

    if table_exists('vax'):
        try:
            with open('vax.csv', 'r') as csv_file:
                data = csv.DictReader(csv_file)
                to_sql = [ (i['date'],i['fips'],i['recip_county'],i['recip_state'], i['series_complete_pop_pct']) for i in data] 
                cursor.executemany(
                    "INSERT INTO vax (date, fips, recip_county, recip_state, series_complete_pop_pct)"
                    " VALUES (?, ?, ?, ?, ?)"
                    , to_sql
                )
                print('Data stored in vax table.')
                os.remove('vax.csv')
        except FileNotFoundError:
            print('extract_vax() was not called. Data not pulled')
    
    db.commit()
    db.close()


def store_cases():

    try:
        if is_cached('cases'):
            print("cases data already cached!")
            return
    except TableNotInitialized as e:
        print(e)
        return

    db = sqlite3.connect('data.db')
    cursor = db.cursor()

    if table_exists('cases'):
        try:
            with open('cases.csv', 'r') as csv_file:
                data = csv.DictReader(csv_file)
                to_sql = [ (i['date'],i['county'],i['state'],i['fips'], i['cases'], i['deaths']) for i in data] 
                cursor.executemany(
                    "INSERT INTO cases (date, county, state, fips, cases, deaths)"
                    " VALUES (?, ?, ?, ?, ?, ?)"
                    , to_sql
                )
            print('Data stored in cases table.')
            os.remove('cases.csv')
        except FileNotFoundError:
            print('extract_cases() was not called. Data not pulled')
    else:
        print('cases table not initialized!')
    
    db.commit()
    db.close()


def store_real_estate():
    db = sqlite3.connect('data.db')
    cursor = db.cursor()

    if table_exists('real_estate'):
        try:
            with open('static_data/real_estate.csv', 'r') as csv_file:
                data = csv.DictReader(csv_file)
                to_sql = [( i['date'], i['fips'], i['state_comp'], i['state_short'], i['median_listing_price'], i['median_change_pct'], i['median_days_on_market'], i['average_listing_price'], i['avg_price_change']) for i in data] 
                cursor.executemany(
                    "INSERT INTO real_estate (date, fips, state_comp, state_short, median_listing_price, median_change_pct,median_days_on_market,average_listing_price,avg_price_change)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    , to_sql
                )
            print('Data stored in real estate table.')
        except FileNotFoundError:
            print('real_estate.csv file not found!')
    else:
        print('real estate table not initialized!')
    
    db.commit()
    db.close()


def table_exists(table):
    db = sqlite3.connect('data.db')
    tables = []

    cursor = db.cursor()
    for row in cursor.execute("SELECT name FROM sqlite_master;"):
        tables.append(row)
    
    db.close()

    if (table,) in tables:
        return True
    return False 

def get_data():
    db = sqlite3.connect('data.db')
    db.row_factory = sqlite3.Row
    cursor = db.cursor()
    cursor.execute("SELECT * FROM cases c JOIN vax v ON c.fips = v.fips JOIN real_estate r ON r.fips = v.fips;")
    result = [dict(row) for row in cursor.fetchall()]
    db.close()
    return result

class TableNotInitialized(Exception):
    pass


app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/api/data', methods=['GET'])
def api_extract_cases():
    return jsonify(get_data())


if __name__ == "__main__":
    init_db()
    extract_cases()
    extract_vax()
    store_cases()
    store_vax()
    store_real_estate()
    app.run()