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
    
    # cdc table already exists in cdc.db
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
    count = 200
    #count = client.get(endpoint, select="COUNT(*)")

    while True:
        results.extend( (client.get(endpoint, limit=chunk, offset=offset)) )
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
        to_sql = [ (i['date'],i['fips'],i['mmwr_week'],i['recip_county'],i['recip_state'],i['completeness_pct'],i['administered_dose1_recip'],i['administered_dose1_pop_pct'],i['administered_dose1_recip_5plus'],i['administered_dose1_recip_5pluspop_pct'],i['administered_dose1_recip_12plus'],i['administered_dose1_recip_12pluspop_pct'],i['administered_dose1_recip_18plus'],i['administered_dose1_recip_18pluspop_pct'],i['administered_dose1_recip_65plus'],i['administered_dose1_recip_65pluspop_pct'],i['series_complete_yes'],i['series_complete_pop_pct'],i['series_complete_5plus'],i['series_complete_5pluspop_pct'],i['series_complete_12plus'],i['series_complete_12pluspop_pct'],i['series_complete_18plus'],i['series_complete_18pluspop_pct'],i['series_complete_65plus'],i['series_complete_65pluspop_pct'],i['booster_doses'],i['booster_doses_vax_pct'],i['booster_doses_12plus'],i['booster_doses_12plus_vax_pct'],i['booster_doses_18plus'],i['booster_doses_18plus_vax_pct'],i['booster_doses_50plus'],i['booster_doses_50plus_vax_pct'],i['booster_doses_65plus'],i['booster_doses_65plus_vax_pct'],i['svi_ctgy'],i['series_complete_pop_pct_svi'],i['series_complete_5pluspop_pct_svi'],i['series_complete_12pluspop_pct_svi'],i['series_complete_18pluspop_pct_svi'],i['series_complete_65pluspop_pct_svi'],i['metro_status'],i['series_complete_pop_pct_ur_equity'],i['series_complete_5pluspop_pct_ur_equity'],i['series_complete_12pluspop_pct_ur_equity'],i['series_complete_18pluspop_pct_ur_equity'],i['series_complete_65pluspop_pct_ur_equity'],i['census2019'],i['census2019_5pluspop'],i['census2019_12pluspop'],i['census2019_18pluspop'],i['census2019_65pluspop']) for i in data] 
        cursor.executemany(
            "INSERT INTO cdc (date,fips,mmwr_week,recip_county,recip_state,completeness_pct,administered_dose1_recip,administered_dose1_pop_pct,administered_dose1_recip_5plus,administered_dose1_recip_5pluspop_pct,administered_dose1_recip_12plus,administered_dose1_recip_12pluspop_pct,administered_dose1_recip_18plus,administered_dose1_recip_18pluspop_pct,administered_dose1_recip_65plus,administered_dose1_recip_65pluspop_pct,series_complete_yes,series_complete_pop_pct,series_complete_5plus,series_complete_5pluspop_pct,series_complete_12plus,series_complete_12pluspop_pct,series_complete_18plus,series_complete_18pluspop_pct,series_complete_65plus,series_complete_65pluspop_pct,booster_doses,booster_doses_vax_pct,booster_doses_12plus,booster_doses_12plus_vax_pct,booster_doses_18plus,booster_doses_18plus_vax_pct,booster_doses_50plus,booster_doses_50plus_vax_pct,booster_doses_65plus,booster_doses_65plus_vax_pct,svi_ctgy,series_complete_pop_pct_svi,series_complete_5pluspop_pct_svi,series_complete_12pluspop_pct_svi,series_complete_18pluspop_pct_svi,series_complete_65pluspop_pct_svi,metro_status,series_complete_pop_pct_ur_equity,series_complete_5pluspop_pct_ur_equity,series_complete_12pluspop_pct_ur_equity,series_complete_18pluspop_pct_ur_equity,series_complete_65pluspop_pct_ur_equity,census2019,census2019_5pluspop,census2019_12pluspop,census2019_18pluspop,census2019_65pluspop)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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