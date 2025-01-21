from abc import abstractmethod
from src.snowf_util import SnowfUtility
import pandas as pd
import logging
from datetime import date
import os
from itertools import product

class Extractor:

    def __init__(self, entity, database, schema, table) -> None:
        self.entity         = entity
        self.database       = database
        self.schema         = schema
        self.table          = table
        self.season_types   = ['Playoffs', 'Regular Season']
    
    @abstractmethod
    def make_request(self) -> pd.DataFrame:
        pass

    def _get_current_season(self):
        '''
        Returns the current season.

        Returns:
            A numeric value for the season year
        '''
        today = date.today()
        return today.year if today > date(today.year, 10, 1) else today.year - 1
    
    def _get_path(self, filename):

        current_file_dir    = os.path.dirname(os.path.abspath(__file__))
        seed_dir            = os.path.abspath(os.path.join(current_file_dir, '../../seed'))
        file_path           = os.path.join(seed_dir, filename + '.csv')

        return file_path

    def generate_params(self, *args):
        """
        Return all combinations from input iterables.  
  
        Args:  
            *args: Multiple iterables, such as list, to combine.  

        Returns:  
            List of lists representing all possible combinations.  

        Example: 
            self.generate_params([2023, 2024], ['Playoff', 'Regular Season']) 
            output: [[2023, 'Playoff'], [2023, 'Regular Season'], [2024, 'Playoff'], [2024, 'Regular Season']].  
        """ 
        for combination in product(*args):
            yield list(combination)

    def _read_file(self, filename) -> pd.DataFrame:
        path    = self._get_path(filename) 
        
        try:
            df = pd.read_csv(path)
        except:
            logging.info('File does not exist')
            df = None

        return df

    def get_snowf_data(self, query, database= '', schema = '', table= ''):
        snowf   = SnowfUtility(database=database, schema=schema, table=table)
        return snowf.query_from_table(query)

    def get_team_id(self) -> list:
        df = self._read_file('teams')
        return list(df['id'])

    def execute(self):
        df = self.make_request()
        if df is None or len(df) < 1:
            logging.info('No data to upload to Snowflake')
            return 

        # df.to_csv(self._get_path(self.entity), index=False)

        snowf = SnowfUtility(database=self.database, schema = self.schema, table = self.table)
        snowf.load_data_to_snowf(df)