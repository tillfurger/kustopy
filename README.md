```bash
pip install kustopy
```

**QueryClient**

Import the query client from kustopy
```python
from kustopy import KustoPyClient as kpc
```

Then set up the client
```python
query_client = kpc.QueryClient(uri='https://sample.kusto.windows.net/',
                               database='confidential-satanalytics-sample',
                               client_id=client_id,
                               client_secret=client_secret,
                               tenant_id=tenant_id,
                               truncation=False)
```

Using Capital Edge of EY, the credentials can be retrieved and the client be set up as
```python
client_id = dbutils.secrets.get(scope="ce5", key="adxClientId")
client_secret = dbutils.secrets.get(scope="ce5", key="adxClientSecret")
tenant_id = dbutils.secrets.get(scope="ce5", key="adxTenantId")

query_client = kpc.QueryClient(uri='https://sample.kusto.windows.net/',
                               database='confidential-satanalytics-sample',
                               client_id=client_id,
                               client_secret=client_secret,
                               tenant_id=tenant_id,
                               truncation=False)
```

We can now get a dataframe of all tables in the database
```python
query_client.get_table_names()
```

and use specific queries to get data from a table to a dataframe
```python
query_client.query_to_df('SampleTable | take 100 | where fruit=="apple" | order by country asc, price desc')
```


**IngestionClient**
following...

