from .extractor import Extractor
from nba_api.stats.endpoints import teamyearbyyearstats
import pandas as pd
import time


class TeamStats(Extractor):
    def __init__(self, entity, database, schema, table) -> None:
        super().__init__(entity, database, schema, table)

    def make_request(self) -> pd.DataFrame:
        team_id         = self.get_team_id()
        team_year_stat  = None

        for id in team_id:
            res         = teamyearbyyearstats.TeamYearByYearStats(team_id=id).get_data_frames()[0]
            # current season stat only, endpoint does not support request by season year
            res         = res.sort_values('YEAR').tail(1)
            # create a primary key
            res['ID']   = res['TEAM_ID'].astype(str) + res['YEAR']
            if team_year_stat is None:
                team_year_stat = res
                continue

            team_year_stat = pd.concat([team_year_stat,res])
            
            time.sleep(1.5)
        
        return team_year_stat
