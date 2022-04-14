Check TODO's if you want to contribute

## Overview

The following functions are available: 

```python
def init_db():
    """
    Initializes the database and create the three tables we'll use: 'cases' , 'vax' , 'real_estate'.
    It also checks if table already exists, if so, it skips.
    """
```
```python
def is_cached(table):
    """
    Checks if data in table is the most recent possible (at least 1 day before current day)
    If this function is called and table does not exist in DB, it raises TableNotInitialized exception
    """
```

```python
def extract_vax():
    """
    Extracts vaccination data from CDC API.
    Writes data to a csv file

    This only runs if:
        - 'vax' table was created (by calling init_db)
        - is_cached('vax') returns False
    """
```
```python
def extract_cases():
    """
    Extracts cases data from New York Times github repo.
    Writes data to a csv file

    This only runs if:
        - 'cases' table was created (by calling init_db)
        - is_cached('cases') returns False
    """
```

```python
def store_vax():
    """
    Stores vaccination data in 'vax' table

    This only runs if:
        - 'vax' table was created (by calling init_db)
        - is_cached('vax') returns False
        - csv file 'vax.csv' was created by extract_vax()
    
    'vax.csv' is removed upon completion
    """
```

```python
def store_cases():
    """
    Stores cases data in 'cases' table

    This only runs if:
        - 'cases' table was created (by calling init_db)
        - is_cached('cases') returns False
        - csv file 'cases.csv' was created by extract_cases()
    
    'cases.csv' is removed upon completion
    """
```
```python
def store_real_estate():
    """
    Stores real estate STATIC data in 'real_estate' table

    This only runs if:
        - 'real_estate' table was created (by calling init_db)
        - csv file 'real_estate.csv' exists
    
    """
```
## Usage

```bash
python3
>>> import data as data
>>> data.extract_vax()
vax table has not been initialized!
>>> data.store_cases()
cases table has not been initialized!
>>> data.init_db()
vax table initialized.
cases table initialized.
real_estate table initialized.
>>> data.extract_vax()
WARNING:root:Requests made without an app_token will be subject to strict throttling limits.
500  rows extracted.
1000  rows extracted.
1500  rows extracted.
2000  rows extracted.
2500  rows extracted.
3000  rows extracted.
3500  rows extracted.
>>> data.store_vax()
Data stored in vax table.
>>> data.store_vax()
vax data already cached!
>>> sqlite3
sqlite> SELECT * FROM vax LIMIT 10;
1|2022-04-13T00:00:00.000|01057|Fayette County|AL|34.9
2|2022-04-13T00:00:00.000|05003|Ashley County|AR|50
3|2022-04-13T00:00:00.000|04019|Pima County|AZ|68.6
4|2022-04-13T00:00:00.000|06043|Mariposa County|CA|
5|2022-04-13T00:00:00.000|06101|Sutter County|CA|61
6|2022-04-13T00:00:00.000|08095|Phillips County|CO|52.2
7|2022-04-13T00:00:00.000|12009|Brevard County|FL|64.5
8|2022-04-13T00:00:00.000|12117|Seminole County|FL|61.9
9|2022-04-13T00:00:00.000|19001|Adair County|IA|49.3
10|2022-04-13T00:00:00.000|19063|Emmet County|IA|52.3
sqlite> 

```