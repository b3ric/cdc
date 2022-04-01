import requests
import csv
import datetime

def extract():
    url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties-recent.csv'

    with requests.Session() as s:
        data = s.get(url)
        decoded_content = data.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        arr = list(cr)

    return arr 

def transform(arr):
    
    arr.pop(0)
    format = '%Y-%m-%d'
    latest = datetime.datetime.strptime(arr[len(arr)-1][0], format)

    clean = []

    for elem in arr:
        date = datetime.datetime.strptime(elem[0], format)
        if date == latest:
            clean.append(elem)
    
    for i in clean:
        print(i)
    
    return clean

def load():
    pass

if __name__ == '__main__':
    arr = extract()
    transform(arr)
