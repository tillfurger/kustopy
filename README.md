# Getting started
kustopy is a Python SDK for Azure Data Explorer (Kusto).

[![PyPI version](https://badge.fury.io/py/kustopy.svg)](https://badge.fury.io/py/kustopy)
[![Downloads](https://pepy.tech/badge/kustopy)](https://pepy.tech/project/kustopy)

Ingestion using pyspark dataframes, is quite slow (compared to ingestion using pandas dataframes). 
But data is immediately in database after the command is run, while for pandas dataframes it can take
while for data to appear.

## Installation and Import
```bash
pip install kustopy
```

```python
from kustopy import KustoPyClient as kpc
```
## Client setup
```python
# Set up the query client
client = kpc.Client(cluster='https://sample.kusto.windows.net/',
                    database='confidential-satanalytics-sample',
                    client_id=client_id,
                    client_secret=client_secret,
                    tenant_id=tenant_id,
                    truncation=False)
```
---
### Queries
```python
# Get list of all tables available in database
client.get_table_names()
```
```python
# Get list of all tables available in database
client.get_schema(tablename='SampleTable')
```

```python
# Write standard kusto queries. To get the response as pandas dataframe set dataframe to True.
client.query(user_input='SampleTable | take 100 | where fruit=="apple"', dataframe=True)
```

---
### Ingestions


```python
# Drop table from the database
client.drop_table(table_name='SampleTable')
```

```python
# Drop table from the database
client.drop_dublicates(table_name='SampleTable')
```

```python
# Write pandas or pyspark dataframe to the database. If the table exists you will get an Error.
client.write_table(dataframe=df, table_name='SampleTable', folder='Sample')
```

```python
# Write pandas or pyspark dataframe to the database. If the table exists it will be replaced, otherwise created.
client.write_replace_table(dataframe=df, table_name='SampleTable', folder='Sample')
```

```python
# Write pandas or pyspark dataframe to the database. If the table exists it will be expanded, otherwise created.
client.write_append_table(dataframe=df, table_name='SampleTable', folder='Sample')
```