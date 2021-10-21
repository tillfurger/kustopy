# Getting started

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
# Get login credential from Azure Vault
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
# Query all tables available in database
query_client.get_table_names()
```

```python
# Write kusto queries to get data into pandas dataframe
query_client.query_to_df('SampleTable | take 100 | where fruit=="apple"')
```

---
### IngestionClient

```python
# Get login credential from Azure Vault
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