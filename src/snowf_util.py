import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from pandas import DataFrame
import os

DEFAULT_CHUNK_SIZE  = 10000

class SnowfUtility():

    def __init__(self, table = '', database= '', schema='') -> None:

        self.database   = database.strip().upper()
        self.schema     = schema.strip().upper()
        self.table      = table.strip().upper()

        try:
            self.connection = snowflake.connector.connect(
                user        = os.getenv('SNOWF_USER'),
                password    = os.getenv('SNOWF_PW'),
                account     = os.getenv('SNOWF_ACCOUNT'),
                role        = os.getenv('SNOWF_ROLE'),
                database    = self.database,
                schema      = self.schema,
                session_parameters={
                    'QUERY_TAG': f'Load {self.table} data'
                }
            )
            
            self.cursor     = self.connection.cursor()
            wh              = os.getenv('SNOWF_TARGET_WH')
            self.cursor.execute(f'use warehouse {wh}')
            
        except Exception as error:
            logging.error(f'Snowflake connection error: {error}')

    def _create_table(self, df: DataFrame):
        
        query   = f'create or replace transient table {self.database}.{self.schema}.{self.table} ( \n' 
        n_cols  = len(df.columns)
        for i, col in enumerate(df.columns):
            col_type = 'VARCHAR'
            if df[col].dtype in ['int64', 'float64']:
                col_type = 'NUMERIC'

            query += col + f' {col_type}'

            if i < n_cols-1:
                query += ', \n'
            else:
                query += '\n)'

        self.cursor.execute(query)

        
    def load_data_to_snowf(self, dframe: DataFrame):
        df         = dframe
        df.columns = [col.upper() for col in df.columns]

        self._create_table(df)

        write_pandas(
            conn                = self.connection
            , df                = df
            , table_name        = self.table
            , database          = self.database
            , schema            = self.schema
            , chunk_size        = DEFAULT_CHUNK_SIZE
        )

        self.connection.close()

    def query_from_table(self, query):
        res     = self.cursor.execute(query)
        data    = res.fetchall()
        self.connection.close()
        
        return data
