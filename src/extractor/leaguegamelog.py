from nba_api.stats.endpoints import leaguegamelog
from .extractor import Extractor
import pandas as pd
import time

class SeasonGames(Extractor):
    def __init__(self, entity, database = '', schema = '', table = ''):
        super().__init__(entity,database, schema, table)

    def make_request(self) -> pd.DataFrame:

        season                  = self._get_current_season()
        games                   = None
        
        for season_type in self.season_types:
            res                 = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type).get_data_frames()[0]
            res['SEASON_TYPE']  = season_type

            if games is None:
                games = res
                continue

            games               = pd.concat([games,res])
            time.sleep(1)

        # set default N/A for null values. DAG merged_table task unable to compare null value to another non-null value
        games.loc[games['WL'].isnull(), 'WL'] = 'N/A'

        return games