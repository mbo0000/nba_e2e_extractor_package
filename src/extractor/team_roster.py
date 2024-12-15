from .extractor import Extractor
from nba_api.stats.endpoints import commonteamroster
import pandas as pd
import time


class TeamRoster(Extractor):
    
    def __init__(self, entity, database, schema, table) -> None:
        super().__init__(entity, database, schema, table)

    def make_request(self) -> pd.DataFrame:
        season              = self._get_current_season()
        teams               = self.get_team_id()
        res                 = None

        for team_id in teams:
            df              = commonteamroster.CommonTeamRoster(team_id=team_id, season=season).get_data_frames()[0]
            df['ID']        = df['SEASON'].astype(str) + df['PLAYER_ID'].astype(str)
            
            if res is None:
                res         = df
                continue

            res = pd.concat([res, df])
        
            time.sleep(1.5)
        
        return res