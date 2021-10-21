from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, DataFormat
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data._models import KustoResultTable
import koalas as ks
import pandas as pd


class PyKusto:

    def __init__(self, uri, database, truncation=True):
        # Define self variables
        self.uri = uri
        self.database = database
        self.truncation = truncation

        # Add ingest uri
        ingest_uri = self.uri.split('//')
        ingest_uri.insert(1, '//ingest-')
        self.ingest_uri = ''.join(ingest_uri)

        # Create client for queries
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(self.uri, self.client_id,
                                                                                    self.client_secret, self.tenant_id)
        self.query_client = KustoClient(kcsb)

        # Create client for data ingestion
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(self.ingest_uri, self.client_id,
                                                                                    self.client_secret, self.tenant_id)
        self.ingestion_client = QueuedIngestClient(kcsb)

    # Function to construct a database query from user input (string)
    def construct_query(self, user_input):
        # If truncation is set to True the user input can be passed right trough
        if self.truncation:
            query = user_input
            try:
                response = self.query_client.execute(self.database, query)
                return response
            except KustoServiceError as e:
                # If the data size is to large, kusto throws an error --> set truncation to False
                raise Exception(
                    f'{e} If you have a query result with data size larger than 50000000 set truncation=False in class.')
        # Otherwise we have to set notrucation in the query
        else:
            try:
                query = f'set notruncation; {user_input}'
                response = self.query_client.execute(self.database, query)
                return response
            except KustoServiceError as e:
                raise Exception(e)

    # Transform the query output in a pandas DataFrame
    def get_pdf(self, user_input):
        response = self.construct_query(user_input)
        return dataframe_from_result_table(response.primary_results[0]).drop(columns=['iris_id', 'iris_metadata'],
                                                                             errors='ignore')

    # Transform the query output in a koalas DataFrame
    def get_kdf(self, user_input):
        # Import the respective packages
        import pandas as pd
        import koalas as ks
        from azure.kusto.data._models import KustoResultTable
        # Get the response of the query
        response = self.construct_query(user_input)
        table = response.primary_results[0]
        # Check if there is a response
        if not table:
            raise ValueError()
        # Check if response is a KustoResultTable
        if not isinstance(table, KustoResultTable):
            raise TypeError("Expected KustoResultTable got {}".format(type(table).__name__))
        # Transform the response in a koalas DataFrame
        columns = [col.column_name for col in table.columns]
        pdf = pd.DataFrame(table.raw_rows, columns=columns).drop(columns=['iris_id', 'iris_metadata'], errors='ignore')
        frame = ks.from_pandas(pdf)
        # Fix Types
        for col in table.columns[:-2]:
            if col.column_type == "bool":
                frame[col.column_name] = frame[col.column_name].astype(bool)
            if col.column_type == "datetime":
                frame[col.column_name] = pd.to_datetime(frame[col.column_name])
            if col.column_type == "timespan":
                frame[col.column_name] = frame[col.column_name].apply(to_pandas_timedelta)
        return frame

    # Replace koalas dataframe to pandas for database upload
    def kdf_to_pdf(self, dataframe):
        if isinstance(dataframe, ks.DataFrame):
            return dataframe.to_pandas()
        else:
            return dataframe

    # Create strings for the "creat table" and "create table ingestion csv mapping" commands
    def ingestion_properties(self, dataframe):
        # Replace koalas dataframe to pandas for database upload
        dataframe = self.kdf_to_pdf(dataframe)
        columns_list = []
        csv_mapping_list = []
        # Translate pandas datatypes to kusto datatypes
        type_map = {'float64': 'real',
                    'float32': 'real',
                    'float': 'real',
                    'geometry': 'string',
                    'object': 'string',
                    'int64': 'int'}
        # Loop through each column and create respective stings
        for i, col in enumerate(dataframe.columns):
            kusto_type = type_map.get(str(dataframe[col].dtype))
            columns_list.append(f'{col}: {kusto_type}')
            csv_mapping_list.append(f'{{"Name": "{col}", "datatype": "{kusto_type}", "Ordinal": {i}}}')
            # Merge them to one string
        columns_string = ', '.join(columns_list)
        csv_mapping_string = ', '.join(csv_mapping_list)
        return columns_string, csv_mapping_string

    # Function to drop tables from database
    def drop_table(self, tablename):
        # Get all the tables in the database
        get_tables_command = '.show tables'
        response = self.query_client.execute_mgmt(self.database, get_tables_command)
        table_exists = any(dataframe_from_result_table(response.primary_results[0])['TableName'] == tablename)
        # If the entered table exists, drop it
        if table_exists:
            try:
                drop_mapping_table_command = f'.drop table {tablename} ingestion csv mapping "{tablename}_CSV_Mapping"'
                response = self.query_client.execute_mgmt(self.database, drop_mapping_table_command)
                print(f'Mapping table "{tablename}_CSV_Mapping" dropped.')
            except KustoServiceError as e:
                raise Exception(e)
            try:
                drop_table_command = f'.drop table {tablename}'
                response = self.query_client.execute_mgmt(self.database, drop_table_command)
                print(f'Table "{tablename}" dropped.')
            except KustoServiceError as e:
                raise Exception(e)
        # Else print that the table doesn't exist
        else:
            print(f'Table "{tablename}" not in database.')

    # Function to write tables to database
    def write_table(self, dataframe, tablename):
        # Replace koalas dataframe to pandas for database upload
        dataframe = self.kdf_to_pdf(dataframe)
        # Get the table creation commands
        columns_string, csv_mapping_string = self.ingestion_properties(dataframe)
        create_table_command = f'.create table {tablename} ({columns_string})'
        create_mapping_command = f'.create table {tablename} ingestion csv mapping \'{tablename}_CSV_Mapping\' \'[{csv_mapping_string}]\''
        try:
            # Create the table
            response = self.query_client.execute_mgmt(self.database, create_table_command)
            response_df_table = dataframe_from_result_table(response.primary_results[0])
            # Create the mapping table
            response = self.query_client.execute_mgmt(self.database, create_mapping_command)
            response_df_mapping = dataframe_from_result_table(response.primary_results[0])
            # Add the data
            ingestion_properties = IngestionProperties(database=self.database, table=tablename,
                                                       data_format=DataFormat.CSV)
            self.ingestion_client.ingest_from_dataframe(dataframe, ingestion_properties=ingestion_properties)
            return print(f'Table "{tablename}" successfully created by the following command: {create_table_command}')
        except KustoServiceError as e:
            raise e

    # Function to write and replace tables to database
    def write_replace_table(self, dataframe, tablename):
        # Replace koalas dataframe to pandas for database upload
        dataframe = self.kdf_to_pdf(dataframe)
        # First drop the table (and the mapping table)
        self.drop_table(tablename)
        # Then create and write the data to the table
        self.write_table(dataframe, tablename)
