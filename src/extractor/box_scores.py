from nba_api.stats.endpoints import boxscoreadvancedv3
from .extractor import Extractor
import pandas as pd
import time

class BoxScore(Extractor):
    def __init__(self, entity, database = '', schema = '', table = ''):
        super().__init__(entity,database, schema, table)

    def _get_games_id(self) -> list:
        db      = 'raw'
        schema  = 'pc_dump'
        # get game id not ingested in db
        query   = '''
            select 
                game_id
            from games
            where
                year >= 2023
                and game_id not in (select gameid from team_boxscore group by 1)
            group by 1
            ;
        '''
        res     = self.get_snowf_data(query, database=db, schema=schema)
        return [game_id[0] for game_id in res]

    def make_request(self) -> pd.DataFrame:

        game_id = self._get_games_id()
        res     = None
        for id in game_id:
            df  = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=f'00{id}').team_stats.get_data_frame()
            print(id, len(df))
            if res is None:
                res = df
                continue
            
            res = pd.concat([res, df])
            time.sleep(1)
  
        return res