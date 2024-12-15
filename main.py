from src.extractor import (
    Context
    , Teams
    , SeasonGames
    , TeamStats
    , TeamRoster
    , PlayerGameStat
    , BoxScore
)

import sys
import logging

entities_map = {
    'teams'                 : Teams
    ,'games'                : SeasonGames
    ,'team_stat'            : TeamStats
    ,'team_roster'          : TeamRoster
    ,'player_game_stat'     : PlayerGameStat
    ,'team_boxscore'        : BoxScore
}

def _get_args():
    if len(sys.argv[1:]) < 4:
        logging.error("ERROR: Missing arguments.")
        return None

    it      = iter(sys.argv[1:])
    args    = dict(zip(it, it))
    return args

def main():
    args        = _get_args()
    entity      = args['--entity']
    ent         = entities_map[entity]
    database    = args['--database']
    schema      = args['--schema']
    table       = args['--table']

    context = Context(ent(entity=entity, database=database, schema=schema, table=table))
    context.extract()
  
if __name__ == '__main__':
    main()