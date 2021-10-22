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
---
### QueryClient
```python
# Get login credentials from Azure Vault
client_id = dbutils.secrets.get(scope="ce5", key="adxClientId")
client_secret = dbutils.secrets.get(scope="ce5", key="adxClientSecret")
tenant_id = dbutils.secrets.get(scope="ce5", key="adxTenantId")

# Set up the query client
query_client = kpc.QueryClient(cluster='https://sample.kusto.windows.net/',
                               database='confidential-satanalytics-sample',
                               client_id=client_id,
                               client_secret=client_secret,
                               tenant_id=tenant_id,
                               truncation=False)
```

```python
# Get list of all tables available in database
query_client.get_table_names()
```

```python
# Write kusto queries to get data into pandas dataframe
query_client.query_to_df(user_input='SampleTable | take 100 | where fruit=="apple"')
```

---
### IngestionClient

```python
# Get login credentials from Azure Vault
client_id = dbutils.secrets.get(scope="ce5", key="adxClientId")
client_secret = dbutils.secrets.get(scope="ce5", key="adxClientSecret")
tenant_id = dbutils.secrets.get(scope="ce5", key="adxTenantId")

# Set up the ingestion client
query_client = kpc.IngestionClient(cluster='https://sample.kusto.windows.net/',
                                   database='confidential-satanalytics-sample',
                                   client_id=client_id,
                                   client_secret=client_secret,
                                   tenant_id=tenant_id,
                                   truncation=False)
```

```python
# Drop table from the database
ingestion_client.drop_table(tablename='SampleTable')
```

```python
# Write pandas dataframe to the database. If the table exists you will get an Error.
ingestion_client.write_table(dataframe=df, tablename='SampleTable')
```

```python
# Write pandas dataframe to the database. If the table exists it will be replaced.
ingestion_client.write_replace_table(dataframe=df, tablename='SampleTable')
```