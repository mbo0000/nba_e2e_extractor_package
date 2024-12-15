from nba_api.stats.static import teams
from .extractor import Extractor
from pandas import DataFrame

class Teams(Extractor):
    def __init__(self, entity, database = '', schema = '', table = ''):
        super().__init__(entity, database, schema, table)

    def make_request(self) -> DataFrame:
        df_teams = DataFrame(teams.get_teams())
        return df_teams
