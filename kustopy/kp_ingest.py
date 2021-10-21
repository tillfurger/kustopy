from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient


def create_engine(cluster, client_id, client_secret, tenant_id):

    # Create ingest uri
    ingest_uri = cluster.split('//')
    ingest_uri.insert(1, '//ingest-')
    ingest_uri = ''.join(ingest_uri)

    # Create connection with string builder
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(ingest_uri,
                                                                                client_id,
                                                                                client_secret,
                                                                                tenant_id)

    return QueuedIngestClient(kcsb)


def ingestion_properties(dataframe):
    columns_list = []
    csv_mapping_list = []
    # Translate pandas datatypes to kusto datatypes
    dtype_map = {'float64': 'real', 'geometry': 'string', 'object': 'string', 'int64': 'int'}
    # Loop through each column and create respective stings
    for i, col in enumerate(dataframe.columns):
        kusto_dtype = dtype_map.get(str(dataframe[col].dtype))
        columns_list.append(f'{col}: {kusto_dtype}')
        csv_mapping_list.append(f'{{"Name": "{col}", "datatype": "{kusto_dtype}", "Ordinal": {i}}}')
    # Merge them to one string
    columns_string = ', '.join(columns_list)
    csv_mapping_string = ', '.join(csv_mapping_list)
    return columns_string, csv_mapping_string