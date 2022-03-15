Python wrapper for CDC API (https://data.cdc.gov/Vaccinations/COVID-19-Vaccinations-in-the-United-States-County/)

## Installation
```bash
pip install pandas
pip install sodapy
```

# Import api from Python console
```python
import api as api
api.init_db()
api.extract_data()
api.store()
```