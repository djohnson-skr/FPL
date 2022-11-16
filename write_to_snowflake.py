import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def main(league, league_entries, matches, standings, draft_order):
    # ----- Rename column names ----- #
    standings = standings.replace({'HaalanD\'or' : 'HaalanDor', 'Please fuck me mikel' : 'mikel', 'Throbbin Williams' : 'Williams'})
    league_entries = league_entries.replace({'HaalanD\'or' : 'HaalanDor', 'Please fuck me mikel' : 'mikel', 'Throbbin Williams' : 'Williams'})
    matches = matches.replace({'HaalanD\'or' : 'HaalanDor', 'Please fuck me mikel' : 'mikel', 'Throbbin Williams' : 'Williams'})
    draft_order = draft_order.replace({'HaalanD\'or' : 'HaalanDor', 'Please fuck me mikel' : 'mikel', 'Throbbin Williams' : 'Williams'})


    # ----- Prep the dataframes for Snowflake ----- #
    league.reset_index(drop=True, inplace=True)
    league_entries.reset_index(drop=True, inplace=True)
    matches.reset_index(drop=True, inplace=True)
    standings.reset_index(drop=True, inplace=True)
    draft_order.reset_index(drop=True, inplace=True)

    ctx = snowflake.connector.connect(
    user = 'PRESETDEMO',
    password = 'iBicwd^mDroEorT7',
    account = 'vxb63237',
    warehouse = 'FPL_WAREHOUSE',
    database = 'FPL',
    schema = 'FPL_SCHEMA'
    )
      
    success_league, nchunks_league, nrows_league, _ = write_pandas(ctx, league, 'LEAGUE', quote_identifiers=False)
    success_leage_entries, nchunks_league_entries, nrows_league_entries, _ = write_pandas(ctx, league_entries, 'LEAGUE_ENTRIES', quote_identifiers=False)
    success_matches, nchunks_matches, nrows_matches, _ = write_pandas(ctx, matches, 'MATCHES', quote_identifiers=False)
    success_standings, nchunks_standings, nrows_standings, _ = write_pandas(ctx, standings, 'STANDINGS', quote_identifiers=False)
    success_draft_order, nchunks_draft_order, nrows_draft_order, _ = write_pandas(ctx, draft_order, 'DRAFT_ORDER', quote_identifiers=False)
    # print(str(success) + ', ' + str(nchunks) + ', ' + str(nrows))

