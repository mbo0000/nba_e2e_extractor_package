from nba_api.stats.endpoints import playergamelog
from .extractor import Extractor
import pandas as pd
import time

class PlayerGameStat(Extractor):
    
    def __init__(self, entity, database, schema, table) -> None:
        super().__init__(entity, database, schema, table)

    def _get_player_id(self):
        db              = 'raw'
        schema          = 'pc_dump'
        query           = f'select player_id from team_roster where season = (select max(season) from team_roster) group by 1;'
        return [col[0] for col in self.get_snowf_data(query, database=db, schema=schema)]

    def make_request(self) -> pd.DataFrame:

        players         = self._get_player_id()
        res             = None

        for player_id, season_type in self.generate_params(players, self.season_types):
            df          = playergamelog.PlayerGameLog(player_id=player_id, season_type_all_star=season_type).get_data_frames()[0]
            df['ID']    =  df['Player_ID'].astype(str) + df['Game_ID'].astype(str)
        
            if res is None:
                res     = df
                continue
                
            res         = pd.concat([res, df])
            time.sleep(1)

        return res