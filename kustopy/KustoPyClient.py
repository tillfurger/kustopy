from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties
from azure.kusto.data.data_format import DataFormat
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.exceptions import KustoServiceError
import logging
from pyspark.sql import DataFrame as psdf
from pandas import DataFrame as pddf
from pyspark.sql.types import StringType, FloatType, IntegerType
from pyspark.sql.types import StructType, StructField


class Client:

    def __init__(self, cluster, database, client_id, client_secret, tenant_id):
        # Define self variables
        self.cluster = cluster
        self.database = database
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id

        # Add ingest cluster
        ingest_cluster = self.cluster.split('//')
        ingest_cluster.insert(1, '//ingest-')
        self.ingest_cluster = ''.join(ingest_cluster)

        # Create client for queries
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(self.cluster, self.client_id,
                                                                                    self.client_secret, self.tenant_id)
        self.query_client = KustoClient(kcsb)

        # Create client for data ingestion
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(self.ingest_cluster, self.client_id,
                                                                                    self.client_secret, self.tenant_id)
        self.ingestion_client = QueuedIngestClient(kcsb)

    # -----------------------------------------QUERY-----------------------------------------

    # Function to construct a database query from user input (string)
    def get_response(self, user_input):
        # If truncation is set to True the user input can be passed right trough
        try:
            response = self.query_client.execute(self.database, user_input)
            return response
        except KustoServiceError as e:
            raise Exception(e)

    # Check what kind of dataframe the input is
    def get_input_type(self, user_input):
        if isinstance(user_input, pddf):
            return 'pandas'
        elif isinstance(user_input, psdf):
            return 'pyspark'
        else:
            raise ValueError("Only pandas or pyspark dataframes allowed as input!")

    # Post a query
    def query(self, user_input, data_frame=True):
        if data_frame:
            response = self.get_response(user_input)
            return dataframe_from_result_table(response.primary_results[0])
        else:
            return self.get_response(user_input)

    # Get a dataframe of all tables in database
    def get_table_names(self):
        get_tables_command = '.show tables'
        response = self.query_client.execute_mgmt(self.database, get_tables_command)
        return dataframe_from_result_table(response.primary_results[0])

    # -----------------------------------------INGESTION-----------------------------------------

    # Create strings for the "creat table" and "create table ingestion csv mapping" commands
    def ingestion_properties(self, data_frame):
        # Get the datatype of the input dataframe
        input_datatype = self.get_input_type(data_frame)
        columns_list = []
        csv_mapping_list = []
        # Translate pandas datatypes to kusto datatypes
        type_map = {'float64': 'real',
                    'float32': 'real',
                    'float': 'real',
                    'geometry': 'string',
                    'object': 'string',
                    'int64': 'int',
                    'int32': 'int',
                    'Int64': 'int',
                    'Int32': 'int'}
        # Check if pandas or pyspark dataframe
        if input_datatype == 'pandas':
            # Loop through each column and create respective stings
            for i, col in enumerate(data_frame.columns):
                kusto_type = type_map.get(str(data_frame[col].dtype))
                columns_list.append(f'{col}: {kusto_type}')
                csv_mapping_list.append(f'{{"Name": "{col}", "datatype": "{kusto_type}", "Ordinal": {i}}}')
                # Merge them to one string
            columns_string = ', '.join(columns_list)
            csv_mapping_string = ', '.join(csv_mapping_list)
            return columns_string, csv_mapping_string
        else:
            # Loop through each column and create respective stings
            for i, col in enumerate(data_frame.columns):
                kusto_type = data_frame.select(col).dtypes[0][1]
                columns_list.append(f'["{col}"]: {kusto_type}')
                # Merge them to one string
            columns_string = ', '.join(columns_list)
            return columns_string, 'no_csv_mapping_necessary'

    def check_if_exists(self, table_name):
        response = self.query_client.execute_mgmt(self.database, '.show tables')
        table_exists = any(dataframe_from_result_table(response.primary_results[0])['TableName'] == table_name)
        return table_exists, response

    def get_table_folder(self, table_name):
        table_exists, response = self.check_if_exists(table_name)
        if table_exists:
            return self.query(f'.show table {table_name} details | project Folder', data_frame=True)['Folder'][0]
        else:
            raise Exception(f"Table '{table_name}' does not exist in database '{self.database}'.")

    # Function to drop tables from database
    def drop_table(self, table_name):
        self.query(f'.drop table {table_name}_CSV_Mapping ifexists')
        self.query(f'.drop table {table_name} ifexists')

    # Function to drop duplicates from table
    def drop_duplicates(self, table_name):
        folder = self.get_table_folder(table_name)
        if folder == '':
            appendix = folder
        else:
            appendix = f'with (folder={folder})'
        self.query(f'.set TempTable {appendix} <| {table_name} | project-away iris_* | distinct *')
        self.query(f'.rename tables TempTable={table_name} ifexists, {table_name}=TempTable')
        self.query('.drop table TempTable ifexists')

    # Function to write tables to database
    def write_table(self, data_frame, table_name, folder='KustoPyTables'):
        # Get the datatype of the input dataframe
        input_datatype = self.get_input_type(data_frame)
        # Get the table creation commands
        columns_string, csv_mapping_string = self.ingestion_properties(data_frame)
        create_table_command = f'.create table {table_name} ({columns_string}) with (folder={folder})'
        create_mapping_command = f'.create table {table_name} ingestion csv mapping \'{table_name}_CSV_Mapping\' \'[{csv_mapping_string}]\' with (folder={folder})'
        # Check if table already exists
        table_exists, response = self.check_if_exists(table_name)
        if table_exists:
            raise Exception(
                f"Table '{table_name}' already exists in database '{self.database}'. If you want to replace the table use write_replace_table().")
        else:
            try:
                # Create the table
                response = self.query_client.execute_mgmt(self.database, create_table_command)
                response_df_table = dataframe_from_result_table(response.primary_results[0])
                if input_datatype == 'pandas':
                    # Create the mapping table
                    response = self.query_client.execute_mgmt(self.database, create_mapping_command)
                    response_df_mapping = dataframe_from_result_table(response.primary_results[0])
                    # Add the data
                    ingestion_properties = IngestionProperties(database=self.database, table=table_name,
                                                               data_format=DataFormat.CSV)
                    self.ingestion_client.ingest_from_dataframe(data_frame, ingestion_properties=ingestion_properties)
                    return logging.info(
                        f"Table '{table_name}' successfully created by the following command: {create_table_command}")
                else:
                    data_frame.write. \
                        format("com.microsoft.kusto.spark.datasource"). \
                        option("kustoCluster", self.cluster). \
                        option("kustoDatabase", self.database). \
                        option("kustoTable", table_name). \
                        option("kustoAadAppId", self.client_id). \
                        option("kustoAadAppSecret", self.client_secret). \
                        option("kustoAadAuthorityID", self.tenant_id). \
                        mode("Append"). \
                        save()
                    return logging.info(
                        f"Table '{table_name}' successfully created by the following command: {create_table_command}")
            except KustoServiceError as e:
                raise e

    # Function to write and replace tables to database
    def write_replace_table(self, data_frame, table_name, folder='KustoPyTables'):
        # First drop the table (and the mapping table)
        try:
            self.drop_table(table_name)
            self.write_table(data_frame, table_name, folder)
            logging.info(f"Table '{table_name}' replaced in '{self.database}'.")
        except FileNotFoundError:
            self.write_table(data_frame, table_name, folder)
            logging.info(f"Table '{table_name}' newly ingested into '{self.database}'.")
    # Function to append to existing table

    def write_append_table(self, data_frame, table_name, folder='KustoPyTables'):
        # Get the datatype of the input dataframe
        input_datatype = self.get_input_type(data_frame)
        # Check if table really exists
        table_exists, response = self.check_if_exists(table_name)
        if table_exists:
            if input_datatype == 'pandas':
              schema_list = []
              type_map = {'float64': FloatType(),
                          'float32': FloatType(),
                          'float': FloatType(),
                          'string': StringType(),
                          'geometry': StringType(),
                          'object': StringType(),
                          'int64': IntegerType(),
                          'int32': IntegerType(),
                          'int': IntegerType()}
              for idx, type in zip(data_frame.dtypes.index, data_frame.dtypes):
                  schema_list.append(StructField(f'{idx}', type_map.get(str(type)), True))
              schema = StructType(schema_list)
              sdf = spark.createDataFrame(data_frame, schema)
              sdf.write. \
                  format("com.microsoft.kusto.spark.datasource"). \
                  option("kustoCluster", self.cluster). \
                  option("kustoDatabase", self.database). \
                  option("kustoTable", table_name). \
                  option("kustoAadAppId", self.client_id). \
                  option("kustoAadAppSecret", self.client_secret). \
                  option("kustoAadAuthorityID", self.tenant_id). \
                  mode("Append"). \
                  save()
            else:
                sdf = data_frame
                sdf.write. \
                  format("com.microsoft.kusto.spark.datasource"). \
                  option("kustoCluster", self.cluster). \
                  option("kustoDatabase", self.database). \
                  option("kustoTable", table_name). \
                  option("kustoAadAppId", self.client_id). \
                  option("kustoAadAppSecret", self.client_secret). \
                  option("kustoAadAuthorityID", self.tenant_id). \
                  mode("Append"). \
                  save()
        else:
            self.write_table(data_frame, table_name, folder)
            logging.info(f"Table '{table_name}' newly ingested into '{self.database}'.")