from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table


def create_engine(cluster, client_id, client_secret, tenant_id):

    # Create connection with string builder
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster,
                                                                                client_id,
                                                                                client_secret,
                                                                                tenant_id)

    return KustoClient(kcsb)


# Function to construct a database query from user input (string)
def construct_query(user_input, client, database, truncation):
    # If truncation is set to True the user input can be passed right trough
    if truncation:
        query = user_input
        try:
            response = client.execute(database, query)
            return response
        except KustoServiceError as e:
            # If the data size is to large, kusto throws an error --> set truncation to False
            raise Exception(f'{e} If you have a query result with data size larger than 50000000 set truncation=False in class.')
    # Otherwise we have to set notrucation in the query
    else:
        try:
            query = f'set notruncation; {user_input}'
            response = client.execute(database, query)
            return response
        except KustoServiceError as e:
            raise Exception(e)


# Transform the query output in a pandas DataFrame
def get_pdf(client, database, user_input, truncation):
    response = construct_query(user_input, client, database, truncation)
    return dataframe_from_result_table(response.primary_results[0]).\
        drop(columns=['iris_id', 'iris_metadata'], errors='ignore').\
        convert_dtypes()
