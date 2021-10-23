# Getting started
kustopy is a Python SDK for Azure Data Explorer (Kusto).

[![PyPI version](https://badge.fury.io/py/kustopy.svg)](https://badge.fury.io/py/kustopy)
[![Downloads](https://pepy.tech/badge/kustopy)](https://pepy.tech/project/kustopy)
## Installation and Import
```bash
pip install kustopy
```

```python
from kustopy import KustoPyClient as kpc
```
## Client setup
```python
# Get login credentials from Azure Vault
client_id = dbutils.secrets.get(scope="ce5", key="adxClientId")
client_secret = dbutils.secrets.get(scope="ce5", key="adxClientSecret")
tenant_id = dbutils.secrets.get(scope="ce5", key="adxTenantId")

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
# Write kusto queries. To get the response as pandas dataframe set dataframe to True.
client.query(user_input='SampleTable | take 100 | where fruit=="apple"', dataframe=True)
```

---
### Ingestions


```python
# Drop table from the database
client.drop_table(tablename='SampleTable')
```

```python
# Drop table from the database
client.drop_dublicates(tablename='SampleTable')
```

```python
# Write pandas dataframe to the database. If the table exists you will get an Error.
client.write_table(dataframe=df, tablename='SampleTable', folder='Sample')
```

```python
# Write pandas dataframe to the database. If the table exists it will be replaced.
client.write_replace_table(dataframe=df, tablename='SampleTable', folder='Sample')
```

If you want to append data to an existing table, we recommend querying the table to a pandas dataframe using
```.query_to_df()```, doing the transformations within pandas and use ```.write_replace_table()``` to overwrite the 
existing table in the database. This gives you full access to the powerful functions of pandas.