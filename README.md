got```bash
pip install kustopy
```

The package includes the functionality to query and to ingest data.

**Query**

Frist load the query function from kustopy
```python
from kustopy import kp_query
```

Then set up the client
```python
query_client = query.create_engine(cluster, client_id, client_secret, tenant_id)
```

For EY on databricks we can get the credentials as
```python
cluster = 'sample01.westeurope'
database = 'sample-db'

client_id = dbutils.secrets.get(scope="ce5", key="adxClientId")
client_secret = dbutils.secrets.get(scope="ce5", key="adxClientSecret")
tenant_id = dbutils.secrets.get(scope="ce5", key="adxTenantId")

query_client = kp_query.create_engine(cluster, client_id, client_secret, tenant_id)
```

Finally we can query the database and get the result into a pandas dataframe
```python

kp_query.get_pdf(user_input='SampleTable | take 10',
                 client=query_client,
                 database=database,
                 truncation=True)
```

